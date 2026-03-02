import asyncio, logging, json, math
import httpx
import asyncssh
from datetime import datetime
from fastapi import FastAPI, Request, Form
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
import config
from pathlib import Path

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Shared states for fans and temperatures
manual_default = {v["name"]:config.MANUAL_DEFAULT_SPEED for v in config.FANS if v["enabled"]}
ilo_state = {
    "fans": {},
    "temperatures": {},
    "last_updated": None,
    "manual_speeds": manual_default,
    "mode": "auto" # auto, manual, silent
}

# Auxiliary functions to manage fans, temps and modes
async def ilo_ssh_command(command: str) -> str:
    """ Execute a command via SSH on the iLO server.
    
    Args:
        command (str): The command to execute on the iLO server.
        
    Returns:
        str: The output of the command if successful.
        logging.error: If the SSH command fails.
    """
    try:
        async with asyncssh.connect(
            config.ILO_HOST,
            username=config.ILO_USER,
            password=config.ILO_PASS,
            known_hosts=None,                           # skip host verification
            kex_algs=["diffie-hellman-group1-sha1"],    # required for older iLOs
            server_host_key_algs=["ssh-rsa"],           # also for compatibility
        ) as conn:
            result = await conn.run(command, check=True)
            logging.info(f"SSH command executed: {command}")
            return result.stdout
    except Exception as e:
        logging.error(f"SSH command failed: {e}")
        return f"Error: {e}"
    
async def change_mode(mode: str):
    """ Change the fan mode to auto, manual, or silent.

    Args:
        mode (str): The desired mode, one of "auto", "silent", or "manual".

    Returns:
        None: If the operation is successful.
        logging.error: If the SSH command fails or invalid mode if passed.
    """

    # Validate the mode
    if mode not in {"auto", "silent", "manual"}:
        return logging.error(f"Invalid mode: {mode}. Expected one of 'auto', 'silent', or 'manual'.")

    if mode == "silent":
        # Change all fans to silent speed
        for fan in config.FANS:
            if fan["enabled"]:
                await ilo_ssh_command(f"fan p {fan['id']} lock {config.SILENT_MODE_SPEED}")
                ilo_state["manual_speeds"][fan["name"]] = config.SILENT_MODE_SPEED

        # Set the mode to silent
        ilo_state["mode"] = "silent"
    elif mode == "manual":
        # Lock fans to manual speeds
        for fan in config.FANS:
            if fan["enabled"]:
                await ilo_ssh_command(f"fan p {fan['id']} lock {ilo_state['manual_speeds'][fan['name']]}")

        # Set the mode to manual
        ilo_state["mode"] = "manual"
    else:
        # Unlock all fans to auto mode, controlled by iLO PIDs
        for fan in config.FANS:
            if fan["enabled"]:
                await ilo_ssh_command(f"fan p {fan['id']} unlock")
        
        # Set the mode to auto
        ilo_state["mode"] = "auto"

async def set_manual_speed(fan_id: int, speed: int):
    """ Set the speed of a fan in manual mode.
    Args:
        fan_id (int): The ID of the fan to set the speed for.
        speed (int): The desired speed as a % (0-100).
    
    Returns:
        None: If the operation is successful.
        logging.warning: If the mode is not manual.
        logging.error: If the speed is out of range or fan ID is invalid."""

    # Check if the mode is manual
    if ilo_state["mode"] != "manual":
        return logging.warning(f"Cannot set manual speed when not in manual mode. Expected mode: manual, but got: {ilo_state['mode']}") 

    # Check if the speed is within valid range
    if not (0 <= speed <= 100):
        return logging.error(f"Invalid speed: {speed}. Must be between 0 and 100.")

    # Find the fan by ID
    fan = next((f for f in config.FANS if f["id"] == fan_id and f["enabled"]), None)
    if not fan:
        return logging.error(f"Invalid fan ID: {fan_id}")
    
    # Convert percentage to 0-255 range
    pwm_value = math.ceil((speed / 100) * 255)

    # Lock the fan to the specified speed
    await ilo_ssh_command(f"fan p {fan['id']} lock {pwm_value}")
    ilo_state["manual_speeds"][fan["name"]] = pwm_value

def get_fan_speeds() -> list:
    """
    Get the current fan speeds from the shared state and format them for display.
    iLO by default returns fan speeds in % (0-100).
    
    Returns:
        list: A list of dictionaries containing fan names and their current speeds.
    """

    # Make a copy of the current fans from the shared state
    fans_state = ilo_state["fans"].copy()
    fans_config = config.FANS.copy()

    # Build fans state list
    fans = [{"verbose-name":fan["verbose-name"], "speed":fans_state[fan["name"]]} for fan in fans_config if fan["enabled"]]

    return fans

def get_temp_readings() -> list:
    """
    Get the current temperature readings from the shared state and format them for display.
    iLO by default returns temperatures in Celsius.

    This function aggregates memory temperatures (DIMM) into a single "Memory" reading.
    It filters out DIMM temperatures from the main temperature readings and calculates the average for memory.
    
    Returns:
        list: A list of dictionaries containing temperature names, values, and safe max limits.
    """

    # Make a copy of the current temperatures from the shared state
    temps_state = ilo_state["temperatures"].copy()
    temps_config = config.TEMPERATURE_SENSORS.copy()
    
    # Aggregate memory temperatures
    mem_values = [int(v) for k, v in temps_state.items() if "DIMM" in k]
    mem_temp = round(sum(mem_values) / len(mem_values), 1) if mem_values else None

    # Build temperature readings list
    temp_readings = []
    for temp in temps_config:
        if temp["enabled"] and "DIMM" not in temp["name"]:
            if temp["name"] in temps_state:
                value = temps_state.get(temp["name"])
                temp_readings.append({
                    "verbose-name": temp["verbose-name"],
                    "value": int(value),
                    "safe-max": temp["safe-max"]
                })
        else:
            logging.debug(f"Skipping sensor '{temp['name']}' in temperature reading.")
    
    # Add aggregated memory temperature if available
    if mem_temp is not None:
        temp_readings.append({"verbose-name": "Memory", "value": mem_temp, "safe-max": 85})
    
    # Sort the readings by verbose name, for consistent display
    temp_readings.sort(key=lambda x: x["verbose-name"])  # Sort by verbose name

    return temp_readings

# Concurrent background tasks for polling iLO data and thermal watchdog
async def poll_ilo_data():
    while True:
        try:
            async with httpx.AsyncClient(verify=False) as client:
                response = await client.get(f"https://{config.ILO_HOST}/redfish/v1/chassis/1/Thermal/", auth=(config.ILO_USER, config.ILO_PASS))
                if response.status_code == 200:
                    logging.debug("Fetched iLO data successfully")

                    # Parse the JSON response
                    response_json = response.json()
                    fans = {fan["FanName"]:fan.get("CurrentReading") for fan in response_json.get("Fans", [])}
                    temperatures = {temp["Name"]:temp.get("ReadingCelsius") for temp in response_json.get("Temperatures", [])}

                    # Update the shared state
                    ilo_state["fans"] = fans
                    ilo_state["temperatures"] = temperatures
                    ilo_state["last_updated"] = datetime.now().strftime("%H:%M:%S")

                    logging.debug(f"Fans: {json.dumps(fans)}")
                    logging.debug(f"Temperatures: {json.dumps(temperatures)}")
                    logging.debug(f"Thermal endpoint hit at {ilo_state['last_updated']}")
                else:
                    logging.error(f"Failed to fetch iLO data: STATUS_CODE={response.status_code} TEXT='{response.text}'")
        except httpx.RequestError as e:
            logging.error(f"HTTP request error: {e}")
        except Exception as e:
            logging.exception(f"Unexpected error during iLO polling: {e}")
        
        # Wait for the next poll interval
        await asyncio.sleep(config.POLL_INTERVAL)

async def thermal_watchdog():
    while True:
        try:
            # Check thermals if in manual or silent mode
            if ilo_state["mode"] in {"manual", "silent"}:
                temps_state = ilo_state["temperatures"].copy()

                # Log the hottest sensor
                hottest = max(temps_state.items(), key=lambda kv: kv[1] or 0)
                logging.info(f"[WATCHDOG] Hottest sensor: {hottest[0]} = {hottest[1]}°C")

                for sensor in config.TEMPERATURE_SENSORS:
                    safe_max = sensor["safe-max"]
                    value = temps_state.get(sensor["name"])

                    # If there is a reading and it exceeds the safe max, switch to auto mode
                    if value is not None and value >= safe_max:
                        logging.warning(
                            f"[WATCHDOG] {sensor['verbose-name']} at {value}°C exceeds safe-max ({safe_max}°C). Forcing auto mode.")
                        await change_mode("auto")
        except Exception as e:
            logging.exception(f"[WATCHDOG] Unexpected error: {e}")
        
        await asyncio.sleep(config.POLL_INTERVAL)

# Start the FastAPI app with lifespan context manager
@asynccontextmanager
async def lifespan(app: FastAPI):
    await change_mode("auto")  # Ensure we start in auto mode
    poll_task = asyncio.create_task(poll_ilo_data())
    watchdog_task = asyncio.create_task(thermal_watchdog())
    yield  # App is running
    poll_task.cancel()
    watchdog_task.cancel()
    try:
        await asyncio.gather(poll_task, watchdog_task)
    except asyncio.CancelledError:
        pass

app = FastAPI(lifespan=lifespan)

templates = Jinja2Templates(directory=config.TEMPLATES_DIR)
app.mount("/static", StaticFiles(directory=config.STATIC_DIR), name="static")

# Serving home page
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "ilo_host": config.ILO_HOST})

# Serving partials for HTMX
@app.get("/temps", response_class=HTMLResponse)
async def temps(request: Request):
    # Get the current temperature readings from the shared state
    temps = get_temp_readings()

    return templates.TemplateResponse("partials/temps.html", {"request": request, "temps": temps})

@app.get("/fans", response_class=HTMLResponse)
async def fans(request: Request):
    # Get the current fan speeds from the shared state
    fans = get_fan_speeds()

    return templates.TemplateResponse("partials/fans.html", {"request": request, "fans": fans})

@app.get("/info", response_class=HTMLResponse)
async def info(request: Request):
    # Make a copy of the current state
    state_copy = ilo_state.copy()
    last_update = state_copy.get("last_updated", "--:--:--")

    info = {
        "current_mode": state_copy["mode"],
        "last_update": last_update
    }
    return templates.TemplateResponse("partials/mode.html", {"request": request, **info})

@app.get("/mode-panel", response_class=HTMLResponse)
async def mode_panel(request: Request):
    return templates.TemplateResponse("partials/mode.html", {
        "request": request,
        "current_mode": ilo_state["mode"],
        "manual_speeds": ilo_state.get("manual_speeds", {}),
        "last_update": ilo_state.get("last_updated", "--:--:--"),
    })

@app.get("/manual-config", response_class=HTMLResponse)
async def manual_config(request: Request):
    fans = [
        {
            "id": fan["id"],
            "name": fan["name"],
            "verbose_name": fan["verbose-name"],
            "speed": math.ceil((ilo_state["manual_speeds"].get(fan["name"]) / 255) * 100)
        }
        for fan in config.FANS if fan["enabled"]
    ]

    return templates.TemplateResponse("partials/manual_config.html", {
        "request": request,
        "current_mode": ilo_state["mode"],
        "fans": fans
    })

# Actuation endpoints
@app.post("/mode", response_class=HTMLResponse)
async def set_mode(request: Request, mode: str = Form(...)):
    # Check if the mode is valid
    if mode not in {"auto", "silent", "manual"}:
        return HTMLResponse("Invalid mode", status_code=400)

    await change_mode(mode)

    # Respond with the updated mode-panel partial
    return templates.TemplateResponse("partials/mode.html", {
        "request": request,
        "current_mode": ilo_state["mode"],
        "manual_speeds": ilo_state.get("manual_speeds", {}),
        "last_update": ilo_state.get("last_updated", "--:--:--"),
    })

@app.post("/fans", response_class=HTMLResponse)
async def manual_fan(request: Request, fan_id: int = Form(...), value: int = Form(...)):

    # Check if current mode is manual
    if ilo_state["mode"] != "manual":
        return HTMLResponse("Cannot set fan speed when not in manual mode.", status_code=400)
    
    # Check if fan_id is valid and enabled
    fan_config = next((f for f in config.FANS if f["id"] == fan_id and f["enabled"]), None)
    if not fan_config:
        return HTMLResponse("Invalid fan ID or disabled fan.", status_code=400)
    
    # Check if value is within the valid range
    if not (0 <= value <= 100):
        return HTMLResponse("Invalid speed value, must be between 0 and 100.", status_code=400)

    fan_name = fan_config["name"]
    logging.info(f"Manual speed set for '{fan_name}' to {value}%")

    # Convert percentage, lock the fan to the specified speed and update the state
    await set_manual_speed(fan_id, value)

    return HTMLResponse("")

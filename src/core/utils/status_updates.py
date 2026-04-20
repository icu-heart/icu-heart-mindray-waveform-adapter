"""
Startup and status logging for containers
"""
import sys
import os
import threading
import time
from datetime import datetime
from colorama import init, Fore, Style
from typing import Callable, Optional, Dict, Any

# Read ASCII from this location
ASCII_PATH = os.path.join(os.path.dirname(__file__), "ascii.txt")
# Default for status update interval (seconds)
STATUS_UPDATE_INTERVAL = 10

# Set colours for different line ranges
COLOUR_MAP = {
    range(1, 6): Fore.BLUE,          
    range(6, 10): Fore.GREEN,        
    range(10, 15): Fore.MAGENTA,    
}

# Lock for coordinated printing
print_lock = threading.Lock()

def safe_print(msg: str, colour: str = "", end: str = "\n"):
    """Thread-safe print with optional colour."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with print_lock:
        sys.stdout.write(colour + f"[{timestamp}] {msg}" + Style.RESET_ALL + end)
        sys.stdout.flush()

def get_colour_for_line(line_number: int) -> str:
    for lines, colour in COLOUR_MAP.items():
        if line_number in lines:
            return colour
    return Fore.WHITE  

def print_banner():
    """Print ascii.txt with each line according to COLOUR_MAP"""
    if not os.path.exists(ASCII_PATH):
        print(Fore.RED + f"[Warning] ascii.txt not found at {ASCII_PATH}" + Style.RESET_ALL)
        return

    with open(ASCII_PATH, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            colour = get_colour_for_line(i)
            sys.stdout.write(colour + line.rstrip("\n") + Style.RESET_ALL + "\n")

def print_config(module_name: str, config_vars: dict, colour=Fore.CYAN):
    """Print config/startup variables."""
    print(colour + module_name + "\n_____________\nConfiguration:\n" + Style.RESET_ALL)
    for name, value in config_vars.items():
        sys.stdout.write(colour + f"  {name:<20}: {value}\n" + Style.RESET_ALL)
    print()  


def start_status_thread(
        module_name: str, 
        get_status: Optional[Callable[[], Dict[str, Any]]] = None, 
        interval: int = STATUS_UPDATE_INTERVAL, 
        colour=Fore.CYAN
    ):
    """
    Start background thread for status update every `interval` seconds.
    get_status is a function which returns a dict of labels and values for
    parameters where status is monitored.
    """

    def status_loop():
        while True:
            time.sleep(interval)
            status = get_status() if callable(get_status) and get_status is not None else {}
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            details = ", ".join(f"{label}: {state}" for label, state in status.items())
            safe_print(f"[STATUS] {module_name} running... {details}", colour)

    t = threading.Thread(target=status_loop, daemon=True) # updates fail if main process fails
    t.start()


def startup(module_name: str, config_vars: dict, get_status: Optional[Callable[[], Dict[str, Any]]] = None):
    """
    Startup sequence

        * Prints banner
        * Prints config info
        * Starts regular status updates
    """
    print_banner()
    print_config(module_name, config_vars)
    start_status_thread(module_name, get_status)

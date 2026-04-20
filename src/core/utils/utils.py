"""
General utility functions
"""
import os
import json
from typing import Dict, Any
from pathlib import Path

def load_registry() -> Dict[str, Any]:
    """Load the registry data from the JSON file."""
    json_path = os.path.join(os.path.dirname(__file__), "../mapping_registry.json")
    with open(json_path, "r") as f:
        return json.load(f)
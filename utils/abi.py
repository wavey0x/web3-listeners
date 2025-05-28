import json
import os

def load_abi(path: str) -> list:
    """Load ABI from JSON file"""
    with open(path, 'r') as f:
        return json.load(f) 
import json

def load_abi(file_path):
    with open(file_path, 'r') as abi_file:
        # Parse the JSON content
        abi = json.load(abi_file)
    return abi
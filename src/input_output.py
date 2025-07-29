import json
import os

def read_json(file_path: str) -> dict:
    """
    This function reads the contents of a JSON file to a dictionary

    Args:
        file_path (str): Path to the JSON file

    Returns:
        dict: Contents of the JSON file as a dictionary
    """
    # error check the input file path
    if not os.path.isfile(file_path) or not file_path.endswith('.json'):
        raise FileNotFoundError(f"The file {file_path} does not exist or is not a JSON file.")

    # open the file and load the JSON data
    with open(file_path, 'r') as file:
        data = json.load(file)

    # return the loaded data
    return data

import csv
import io
import json
import os
import sys
import pandas
from datetime import datetime

AZURE_STORAGE_CONNECTION_STRING = "AZURE_STORAGE_CONNECTION_STRING"

def get_config(path):
    if not os.path.exists(path):
        print("config file not found")
        return False

    with open(path, "r") as f:
        return json.load(f)


def get_filename(path):
    return os.path.splitext(os.path.basename(path))[0]


def get_output_path(blob_name, output_path):
    filename = get_filename(blob_name)
    op_path = os.path.join(output_path, f"{filename}.json").replace("\\", "/")
    print(f"Writing to {op_path}")
    return op_path


def get_time():
    now = datetime.now()
    return now.strftime("%Y-%m-%d %X")

def get_fields(data):
    return data.splitlines()[0].decode("utf-8").split(",")


def process(data, metadata):
    """Adds metadata tags"""
    parsed_data = pandas.read_csv(io.StringIO(data.decode("utf-8")))

    updated_data = list()
    for _, row in parsed_data.iterrows():
        updated_data.append({"_m": metadata, "_p": {"data": dict(row)}})
    return updated_data

def custom_json_dump(processed_data):
    """
    Takes in processed data and returns a custom JSON format with
    each line representing valid JSON.
    """
    return "\n".join(list(map(json.dumps,  processed_data)))

class Cloud:
    AWS = "AWS"
    GCP = "GCP"
    AZURE = "AZURE"

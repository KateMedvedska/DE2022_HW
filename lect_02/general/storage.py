"""
Layer of persistence. Save content to outer world here.
"""

from os import makedirs
from typing import Union, List, Dict
from fastavro import writer, parse_schema, validate
import json


def save_to_disk_json(json_data: str, path: str, name: str) -> None:
    # create dirs if not exist
    makedirs(path, exist_ok=True)

    # save JSON to disk
    with open(path + '/' + name + '.json', 'w') as write_file:
        json.dump(json_data, write_file)


def read_from_disk(file_path: str, file_extension: str) -> Union[List[Dict], Dict]:
    if file_extension == 'json':
        with open(file_path + '.json', 'r') as read_file:
            data = json.load(read_file)
    else:
        raise f'Incorrect file_extension {file_extension}'

    return data


def save_to_disk_avro(json_data: Union[List[Dict], Dict], path: str, name: str) -> None:
    # create dirs if not exist
    makedirs(path, exist_ok=True)

    schema = {
        'name': 'Sales',
        'type': 'record',
        'fields': [
            {'name': 'client', 'type': 'string'},
            {'name': 'purchase_date', 'type': 'string'},
            {'name': 'product', 'type': 'string'},
            {'name': 'price', 'type': 'int'},
        ],
    }
    parsed_schema = parse_schema(schema)

    # Writing to avro
    with open(path + '/' + name + '.avro', 'wb') as write_file:
        writer(write_file, parsed_schema, json_data)

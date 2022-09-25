"""
This file contains the controller that accepts transformation json to avro file
and trigger business logic layer
"""

import os
from flask import Flask, request
from flask import typing as flask_typing
from lect_02.general import storage


app = Flask(__name__)


@app.route('/', methods=['POST'])
def main() -> flask_typing.ResponseReturnValue:
    """
    Controller that accepts transformation json to avro file
    trigger business logic layer

    Proposed POST body in JSON:
    {
      "data: "2022-08-09",
      "raw_dir": "/path/to/my_dir/raw/sales/2022-08-09"
      "stg_dir": "/path/to/my_dir/stg/sales/2022-08-09"
    }
    """
    input_data: dict = request.json
    date = input_data.get('date')
    raw_dir = input_data.get('raw_dir')
    stg_dir = input_data.get('stg_dir')

    if not date:
        return {
            "message": "date parameter missed",
        }, 400

    if not raw_dir:
        return {
            "message": "raw_dir parameter missed",
        }, 400

    if not stg_dir:
        return {
            "message": "stg_dir parameter missed",
        }, 400

    full_path = raw_dir + '/sales_' + date
    json_data = storage.read_from_disk(file_path=full_path, file_extension='json')

    name_to = 'sales_'+date
    storage.save_to_disk_avro(json_data=json_data, path=stg_dir, name=name_to)


    return {
        "message": "JSON data successfully transformation to avro",
    }, 201


if __name__ == "__main__":
    app.run(debug=True, host="localhost", port=8082)

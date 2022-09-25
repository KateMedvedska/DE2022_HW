
import os
from general import api_client
from general import storage

AUTH_TOKEN = os.environ.get("API_AUTH_TOKEN")


def run_job1(date: str = '', raw_dir: str = ''):
    print("Starting job1:")
    api_client.get_sales(auth=AUTH_TOKEN, date=date, raw_dir=raw_dir)
    print("job1 completed!")


def run_job2(date: str = '', raw_dir: str = '', stg_dir: str = ''):
    print("Starting job2:")
    full_path = raw_dir + '/sales_' + date
    json_data = storage.read_from_disk(file_path=full_path, file_extension='json')

    name_to = 'sales_' + date
    storage.save_to_disk_avro(json_data=json_data, path=stg_dir, name=name_to)
    print("job2 completed!")


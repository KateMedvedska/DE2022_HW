import requests
from typing import List
from lect_02.ht_lect_02.general.storage import save_to_disk_json

API_URL = 'https://fake-api-vycpfa6oca-uc.a.run.app/'


def get_sales(auth: str, date: str, raw_dir: str) -> None:
    json_data = []

    # 1. get data from the API
    result_status_code = 200
    page = 1
    # while pagination
    while result_status_code == 200:
        response = requests.get(
            url=API_URL + '/sales',
            params={'date': date, 'page': page},
            headers={'Authorization': auth}
        )

        # check response status_code
        result_status_code = response.status_code
        if result_status_code == 200:
            cur_json_data = response.json()
        else:
            continue
        # check response type
        if isinstance(cur_json_data, List):
            json_data.extend(cur_json_data)
        else:
            raise f"Error type response data. return type {type(cur_json_data)}, must be List."

        # next page
        page += 1

    # 2. save data to disk:
    f_name = 'sales_' + date
    save_to_disk_json(json_data=json_data, path=raw_dir, name=f_name)


import os
import time
import requests


BASE_DIR = os.environ.get("BASE_DIR")

if not BASE_DIR:
    print("BASE_DIR environment variable must be set")
    exit(1)

JOB1_PORT = 8081
JOB2_PORT = 8082

DATE_STR = "2022-08-09"
RAW_DIR = os.path.join(BASE_DIR, "raw", "sales", DATE_STR)
STG_DIR = os.path.join(BASE_DIR, "stg", "sales", DATE_STR)


def run_job1():
    print("Starting job1:")
    resp = requests.post(
        url=f"http://localhost:{JOB1_PORT}/",
        json={
            "date": DATE_STR,
            "raw_dir": RAW_DIR
        }
    )
    if resp.status_code != 201:
        print(resp.text)
    assert resp.status_code == 201
    print("job1 completed!")


def run_job2():
    print("Starting job2:")
    resp = requests.post(
        url=f'http://localhost:{JOB2_PORT}/',
        json={
            "date": DATE_STR,
            "raw_dir": RAW_DIR,
            "stg_dir": STG_DIR
        }
    )
    if resp.status_code != 201:
        print(resp.text)
    assert resp.status_code == 201
    print("job2 completed!")


if __name__ == '__main__':
    run_job1()
    time.sleep(3)
    run_job2()

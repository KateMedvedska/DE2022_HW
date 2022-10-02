"""
Upload a file to GCS

Notes:
    - all examples can be found here:
    https://github.com/googleapis/python-storage/tree/main/samples/snippets
"""
from google.cloud import storage
from os import listdir
from os.path import isfile, join
import logging


def upload_blob(bucket_name, source_file_path, destination_blob_name):
    """
    Uploads a file to the bucket.
    """
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client(project='de2022-kate-medvedska')
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_path)

    print(
        f"File {source_file_path} uploaded to {destination_blob_name}."
    )


def upload_folder(bucket_name, source_folder_path, destination_folder_name):
    """
    Uploads all files in folder to the bucket.
    """

    # get list of files in folder
    list_files = [f for f in listdir(source_folder_path) if isfile(join(source_folder_path, f))]
    logging.info(f"list all files: {str(list_files)}")

    # upload all files to gcs
    for cur_file in list_files:
        upload_blob(
            bucket_name=bucket_name,
            source_file_path=source_folder_path + '/' + cur_file,
            destination_blob_name=destination_folder_name + '/' + cur_file
        )

    logging.info(
        f"All files in folder {source_folder_path} uploaded to {destination_folder_name}."
    )


if __name__ == "__main__":
    upload_blob(
        bucket_name='de2022-hw-lect-10',
        source_file_path='/Users/k.medvedska/Repositories/DE2022_HW/cache/sales/2022-08-01/src1_sales_2022-08-01__01.csv',
        destination_blob_name='2022-08-01/src1_sales_2022-08-01__01.csv',
    )

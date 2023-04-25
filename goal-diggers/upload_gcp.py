import os

from google.cloud import storage

os.environ["no_proxy"]="*" # set this for airflow errors. https://github.com/apache/airflow/discussions/24463

def create_dir(parent_dir, directory):
    path = os.path.join(parent_dir, directory)
    os.makedirs(path, exist_ok=True)

def write_json_to_gcs(bucket_name, blob_name, service_account_key_file, static_review_file):
    """Write and read a blob from GCS using file-like IO"""
    storage_client = storage.Client.from_service_account_json(service_account_key_file)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(static_review_file)
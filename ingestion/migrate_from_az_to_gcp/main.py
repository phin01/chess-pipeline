
#%%
import ast
import json
from datetime import datetime
import os
from google.cloud import storage
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import argparse
    
parser = argparse.ArgumentParser()
parser.add_argument("--start", help="Start date for migrating blobs")
parser.add_argument("--end", help="End date for migrating blobs")
args = parser.parse_args()

load_dotenv(dotenv_path=".env")
AZURE_STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT")
AZURE_STORAGE_KEY = os.getenv("AZURE_STORAGE_KEY")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCP_BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
START_DATE = args.start if args.start else os.getenv("START_DATE")
END_DATE = args.end if args.end else os.getenv("END_DATE")


#%%

# Original files in azure are in .txt format, with data as python dict strings
# GPC files will contain actual JSON objects, one per line
# This function reads the blob and returns the formatted content for upload in GCP
def process_azure_blob(container_client, blob_name):
    
    print(f"Processing {blob_name}")
    blob_client = container_client.get_blob_client(blob_name)
    blob_content = blob_client.download_blob().readall().decode("utf-8")

    blob_lines = blob_content.strip().split("\n")
    puzzle_data = []

    for line in blob_lines:
        if not line.strip():
            continue
        
        puzzle_record = ast.literal_eval(line)
        puzzle_data.append(json.dumps(puzzle_record))

    return "\n".join(puzzle_data)


def upload_to_gcp(gcp_bucket, blob_name, content):
    blob_path = f"dt={blob_name}/{blob_name}.json"
    blob = gcp_bucket.blob(blob_path)
    blob.upload_from_string(content)
    print(f"Uploaded {blob_name} to GCP")


def should_upload_blob(blob_name):
    if not blob_name.endswith("_v2.txt"):
        return False, None
    file_date = blob_name.replace("_v2.txt", "").replace("raw/", "")
    return START_DATE <= file_date <= END_DATE, file_date



#%%
def main():

    azure_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_KEY)
    container_client = azure_client.get_container_client(AZURE_STORAGE_ACCOUNT)

    gcs_client = storage.Client()
    gcp_bucket = gcs_client.bucket(GCP_BUCKET_NAME)

    blobs = container_client.list_blobs()
    for blob in blobs:
        should_upload, file_date = should_upload_blob(blob.name)
        if should_upload:
            content = process_azure_blob(container_client, blob.name)
            upload_to_gcp(gcp_bucket, file_date, content)


if __name__ == "__main__":
    main()
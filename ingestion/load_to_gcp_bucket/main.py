#%%
import os
import json
from dotenv import load_dotenv
import requests as r
from datetime import datetime, timezone
from google.cloud import storage

load_dotenv(dotenv_path=".env")
USERNAME = os.getenv('CHESS_USERNAME')
API_URL = os.getenv('API_URL')
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
GCP_BUCKET_NAME = os.getenv('GCP_BUCKET_NAME')

def load_data_from_api():
    dados = r.get(f'{API_URL}/{USERNAME}')
    html_content = dados.content.decode('utf-8')
    return html_content

def format_puzzle_data(html_content):
    json_content = json.loads(html_content)
    recent_problems = json_content.get("recentRatedProblems", [])
    lines = [json.dumps(p) for p in recent_problems]
    return "\n".join(lines)

def generate_blob_name():
    now = datetime.now(timezone.utc)
    filename = f"dt={now.date()}/{now.date()}.json"
    return filename

def upload_to_gcp_bucket(json_data, destination_blob_name):
    storage_client = storage.Client(project=GCP_PROJECT_ID)
    bucket = storage_client.bucket(GCP_BUCKET_NAME)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(
        data=json_data,
        content_type="application/json"
    )

def main():
    html_content = load_data_from_api()
    formatted_data = format_puzzle_data(html_content)
    blob_name = generate_blob_name()
    upload_to_gcp_bucket(formatted_data, blob_name)


if __name__ == "__main__":
    main()
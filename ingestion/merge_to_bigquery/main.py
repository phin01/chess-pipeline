#%%
import os
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("BQ_DATASET", "chess_puzzles")
STAGING_TABLE = f"{PROJECT_ID}.{DATASET}.stg_api_data"
RAW_TABLE = f"{PROJECT_ID}.{DATASET}.raw_chess_puzzles"
BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
LOAD_PREVIOUS_DAYS = int(os.getenv("LOAD_PREVIOUS_DAYS", 3))

# %%
# Defaults to loading the previous 3 days unless date range or load days are specified
def get_load_range():
    start = os.getenv("START_DATE")
    end = os.getenv("END_DATE")

    if start and end:
        return (
            datetime.fromisoformat(start).date(),
            datetime.fromisoformat(end).date()
        )

    today = datetime.now(timezone.utc).date()
    return today - timedelta(days=LOAD_PREVIOUS_DAYS), today

def get_bucket_uris(bucket, start_date, end_date):
    current = start_date
    uris = []

    while current <= end_date:
        uris.append(f"gs://{bucket}/dt={current}/*.json")
        current += timedelta(days=1)

    if not uris:
        raise ValueError("No URIs generated")

    return uris

# Loads files from bucket to BigQuery 'temporary' staging table
# Overwrites table on each run
# Autodetect schema because it could change without notice
def load_to_staging(client, uris):
    print(f"Loading URIs: {uris}")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    load_job = client.load_table_from_uri(
        uris,
        STAGING_TABLE,
        job_config=job_config,
    )

    load_job.result()
    print("Loaded data into staging table")


# Merge staging table into raw table
# Only inserts new records when puzzle_id is not found
# Staging table may contain duplicates, SELECT DISTINCT handles that
# Full payload is stored as JSON, as we have no control over API schema
# This raw table is considered the source of truth for all puzzles
def merge_to_raw(client):
    merge_query = f"""
    MERGE `{RAW_TABLE}` T
    USING (SELECT DISTINCT * FROM `{STAGING_TABLE}`) S
    ON T.puzzle_id = CAST(S.id AS STRING)

    WHEN NOT MATCHED THEN
      INSERT (puzzle_id, payload, ingested_at)
      VALUES (CAST(S.id AS STRING), TO_JSON(S), CURRENT_TIMESTAMP())
    """

    query_job = client.query(merge_query)
    query_job.result()
    print("Raw table merge completed")


# %%
def main():
    client = bigquery.Client()
    start_date, end_date = get_load_range()
    print(f"Loading from {start_date} to {end_date}")
    uris = get_bucket_uris(BUCKET_NAME, start_date, end_date)
    load_to_staging(client, uris)
    merge_to_raw(client)

if __name__ == "__main__":
    main()
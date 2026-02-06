from google.cloud import bigquery
import logging
from typing import List, Optional
from config import AimConfig

def get_bq_client(config: AimConfig) -> bigquery.Client:
    """Returns an authenticated (unless dry run mocks it) BQ Client."""
    # In Dry Run, we still init the client because usually we mock the execution calls.
    # Or should we return a Mock if dry_run?
    # The pattern in main.py was to use real client but check DRY_RUN before executing.
    # We will stick to that safe pattern.
    return bigquery.Client(project=config.project_id)

def execute_query_from_file(client: bigquery.Client, file_path: str, dry_run: bool):
    """Reads SQL from file and executes it."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            query = f.read()
            
        logging.info(f"📜 Executing SQL from {file_path}...")
        
        if not dry_run:
            query_job = client.query(query)
            query_job.result() # Wait for completion
            logging.info("✅ SQL executed successfully.")
        else:
            logging.info("🚧 DRY RUN: Would execute SQL.")
            
    except FileNotFoundError:
        logging.error(f"❌ SQL file not found: {file_path}")
        raise
    except Exception as e:
        logging.error(f"❌ Failed to execute SQL: {e}")
        raise

def load_table_from_dataframe(client, df, table_ref, job_config, dry_run):
    if not dry_run:
        load_job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        load_job.result()
        logging.info(f"✅ Uploaded {df.shape[0]} rows to {table_ref}")
    else:
        logging.info(f"🚧 DRY RUN: Would upload {df.shape[0]} rows to {table_ref}")

def load_table_from_uri(client, uri, table_ref, job_config, dry_run):
    if not dry_run:
        load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
        load_job.result()
        logging.info(f"✅ Loaded {uri} into {table_ref}")
    else:
        logging.info(f"🚧 DRY RUN: Would load {uri} into {table_ref}")

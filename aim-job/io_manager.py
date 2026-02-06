from file_io.backend import IOBackend
from file_io.local_backend import LocalBackend
from file_io.gcs_backend import GCSBackend
from config import AimConfig
from google.cloud import storage
import pandas as pd
from io import StringIO
import logging

def get_io_backend(config: AimConfig) -> IOBackend:
    if config.aim_mode == "local":
        return LocalBackend(config.local_root)
    else:
        # Default bucket for output is aim_bucket_name
        return GCSBackend(config.project_id, config.aim_bucket_name, config.aim_gcs_prefix)

def load_priority_runlist(config: AimConfig, io_impl: IOBackend) -> pd.DataFrame:
    """
    Loads priority runlist DF.
    Logic:
    - If Local: io_impl.read_text("runlist/priority_runlist_current.csv")
    - If Cloud: The Config has a GCS URI (possibly different bucket).
      We can't blindly use io_impl if the bucket differs.
      We must check if the URI matches our backend's bucket.
      If not, we instantiate a temporary GCS reader or use a raw client.
    """
    try:
        content = ""
        uri = config.priority_runlist_gcs_uri
        
        if config.aim_mode == "local":
             # Convention: local files are in runlist/
             local_path = "runlist/priority_runlist_current.csv"
             if io_impl.exists(local_path):
                 logging.info(f"⬇️ Loading local runlist from {local_path}...")
                 content = io_impl.read_text(local_path)
             else:
                 logging.error(f"❌ Local runlist not found at {local_path}")
                 return None
        else:
             # Cloud Mode
             logging.info(f"⬇️ Loading runlist from {uri}...")
             if not uri.startswith("gs://"):
                  raise ValueError("Runlist URI must start with gs:// in Cloud Mode")
             
             parts = uri.replace("gs://", "").split("/", 1)
             bucket = parts[0]
             blob_name = parts[1]
             
             # Instead of returning immediately, we get the text content
             content = _get_raw_text_gcs(config.project_id, bucket, blob_name)

        if not content:
            return None
            
        df = pd.read_csv(StringIO(content))
        
        # Normalize Columns
        # 1. Strip whitespace from headers
        df.columns = df.columns.str.strip()
        
        # 2. Case-insensitive mapping to canonical names
        # We expect: Vehicle, Size, PriorityRank (or Rank)
        col_map = {c: c.lower() for c in df.columns}
        reverse_map = {}
        for orig, lower in col_map.items():
            if lower == "vehicle": reverse_map[orig] = "Vehicle"
            elif lower == "size": reverse_map[orig] = "Size"
            elif lower in ["priorityrank", "rank"]: reverse_map[orig] = "PriorityRank"
            
        df.rename(columns=reverse_map, inplace=True)
        
        # 3. Validate and Clean
        required = ["Vehicle", "Size"]
        if not all(col in df.columns for col in required):
            logging.error(f"❌ Runlist missing required columns {required}. Found: {df.columns.tolist()}")
            return None
            
        # Drop invalid rows
        df = df[~df["Vehicle"].str.lower().isin(["nan", "", "none"])]
        df = df[~df["Size"].str.lower().isin(["nan", "", "none"])]
        
        return df

    except Exception as e:
        logging.error(f"❌ Failed to load runlist: {e}")
        return None

def _get_raw_text_gcs(project_id, bucket_name, blob_name):
     client = storage.Client(project=project_id)
     bucket = client.bucket(bucket_name)
     blob = bucket.blob(blob_name)
     return blob.download_as_text()

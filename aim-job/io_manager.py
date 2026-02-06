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
             
             # Check if our backend matches this bucket
             if isinstance(io_impl, GCSBackend) and io_impl.bucket_name == bucket:
                 # We can try to assume it's reachable, but GCSBackend adds root_prefix.
                 # Runlist might be in a different folder.
                 return _load_df_raw_gcs(config.project_id, bucket, blob_name)
             else:
                 return _load_df_raw_gcs(config.project_id, bucket, blob_name)

        if not content:
            return None
            
        return pd.read_csv(StringIO(content))

    except Exception as e:
        logging.error(f"❌ Failed to load runlist: {e}")
        return None

def _load_df_raw_gcs(project_id, bucket_name, blob_name):
     client = storage.Client(project=project_id)
     bucket = client.bucket(bucket_name)
     blob = bucket.blob(blob_name)
     txt = blob.download_as_text()
     return pd.read_csv(StringIO(txt))

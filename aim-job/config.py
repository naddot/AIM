import os
import json
import logging
from dataclasses import dataclass, field
from typing import List, Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

@dataclass
class AimConfig:
    # Environment / Infrastructure
    project_id: str = os.getenv("PROJECT_ID", "bqsqltesting")
    aim_mode: str = os.getenv("AIM_MODE", "cloud").lower()
    dry_run: bool = os.getenv("DRY_RUN", "False").lower() in ("true", "1", "t")
    local_root: str = os.getenv("AIM_LOCAL_ROOT", "./demo")
    
    # GCS Buckets & Paths
    tyrescore_bucket: str = os.getenv("TYRESCORE_BUCKET", "tyrescore")
    tyrescore_prefix: str = os.getenv("TYRESCORE_PREFIX", "tyrescore-AWS3-daily-files/")
    tyrescore_file_extension: str = os.getenv("TYRESCORE_FILE_EXTENSION", ".csv")
    
    aim_bucket_name: str = os.getenv("AIM_BUCKET_NAME", "aim-home")
    aim_gcs_prefix: str = os.getenv("AIM_GCS_PREFIX", "aim-daily-files")
    config_gcs_uri: Optional[str] = os.getenv("CONFIG_GCS_URI")
    priority_runlist_gcs_uri: str = os.getenv("AIM_PRIORITY_RUNLIST_GCS_URI", "gs://aim-home/aim-priority-runlist/AIM rankings priority_runlist_current.csv")
    ignore_gcs_config: bool = os.getenv("IGNORE_GCS_CONFIG", "False").lower() in ("true", "1", "t")

    # BigQuery
    aim_dataset_id: str = os.getenv("AIM_DATASET_ID", "AIM")
    aim_table_id: str = os.getenv("AIM_TABLE_ID", "AIMData")
    cam_table_id: str = os.getenv("CAM_TABLE_ID", "bqsqltesting.CAM_files.CAM_SKU")
    bq_write_disposition: str = os.getenv("AIM_BQ_WRITE_DISPOSITION", "WRITE_TRUNCATE")

    # AIM Service
    aim_base_url: str = os.getenv("AIM_BASE_URL", "https://aim-engine-829092209663.europe-west1.run.app")
    aim_waves_url: str = os.getenv("AIM_WAVES_URL", "https://aim-engine-829092209663.europe-west1.run.app")
    aim_service_password: str = os.getenv("AIM_SERVICE_PASSWORD", "!BlU35qU4R3!")
    
    # Run Parameters
    run_mode: str = os.getenv("AIM_RUN_MODE", "PER_SEGMENT").upper()
    total_overall: int = int(os.getenv("AIM_TOTAL_OVERALL", "10000"))
    batch_size: int = int(os.getenv("AIM_BATCH_SIZE", "500"))
    
    # Tuning Parameters (Overrides possible via GCS)
    page_size: int = int(os.getenv("AIM_PAGE_SIZE", "45"))
    total_per_segment: int = int(os.getenv("AIM_TOTAL_PER_SEGMENT", "500"))
    parallel_segments: int = int(os.getenv("AIM_PARALLEL_SEGMENTS", "7"))
    requests_per_segment: int = int(os.getenv("AIM_REQUESTS_PER_SEGMENT", "4"))
    request_timeout_s: int = int(os.getenv("AIM_REQUEST_TIMEOUT_S", "900"))
    
    goldilocks_zone_pct: int = int(os.getenv("AIM_GOLDILOCKS_ZONE_PCT", "15"))
    price_fluct_upper: float = float(os.getenv("AIM_PRICE_FLUCT_UPPER", "1.1"))
    price_fluct_lower: float = float(os.getenv("AIM_PRICE_FLUCT_LOWER", "0.9"))
    
    brand_enhancer: str = os.getenv("AIM_BRAND_ENHANCER", "").strip()
    model_enhancer: str = os.getenv("AIM_MODEL_ENHANCER", "").strip()
    season: str = os.getenv("AIM_SEASON", "").strip()
    
    limit_segments: List[str] = field(default_factory=list)

    def __post_init__(self):
        # Post-initialization logic
        if self.aim_mode == "local":
            self.aim_base_url = self.aim_waves_url
            
        limit_env = os.getenv("AIM_LIMIT_SEGMENTS", "").strip()
        if limit_env and not self.limit_segments:
            self.limit_segments = [s.strip() for s in limit_env.split(",") if s.strip()]


def load_config() -> AimConfig:
    """Loads configuration from Env and applies GCS overrides if configured."""
    # 1. Init from Env
    conf = AimConfig()
    
    # 2. Config Override (GCS or Local)
    if conf.ignore_gcs_config:
        logging.info("ℹ️ IGNORE_GCS_CONFIG is True. Skipping GCS config load.")
        return conf

    overrides = {}
    
    # Determine where to look for overrides
    if conf.aim_mode == "local":
        config_path = os.path.join(conf.local_root, "config", "aim-config.json")
        try:
            if os.path.exists(config_path):
                logging.info(f"⬇️ Loading local config from {config_path}...")
                with open(config_path, "r") as f:
                    overrides = json.load(f)
            else:
                logging.warning(f"⚠️ Local config file {config_path} not found.")
        except Exception as e:
            logging.error(f"❌ Failed to load local config: {e}")
    
    elif conf.config_gcs_uri:
        # Load From GCS
        try:
            from google.cloud import storage
            client = storage.Client(project=conf.project_id)
            
            uri = conf.config_gcs_uri
            if uri.startswith("gs://"):
                parts = uri.replace("gs://", "").split("/", 1)
                bucket = client.bucket(parts[0])
                blob = bucket.blob(parts[1])
                if blob.exists():
                    logging.info(f"⬇️ Downloading config from {uri}...")
                    overrides = json.loads(blob.download_as_text())
                else:
                    logging.warning(f"⚠️ Config {uri} not found.")
        except Exception as e:
            logging.error(f"❌ Failed to load GCS config: {e}")

    # 3. Apply Overrides
    if overrides:
        _apply_overrides(conf, overrides)
        logging.info("✅ Configuration overrides applied.")

    return conf

def _apply_overrides(conf: AimConfig, overrides: dict):
    """Helper to map JSON keys to Config fields safely."""
    
    def set_if(key, attr, factory=None):
        if key in overrides:
            try:
                val = overrides[key]
                if factory:
                    val = factory(val)
                setattr(conf, attr, val)
                logging.info(f"   🔹 Overriding {attr}: {val}")
            except Exception as e:
                logging.warning(f"   ⚠️ Invalid config value for {key}: {e}")

    set_if("TOTAL_PER_SEGMENT", "total_per_segment", int)
    set_if("GOLDILOCKS_ZONE_PCT", "goldilocks_zone_pct", int)
    set_if("PRICE_FLUCTUATION_UPPER", "price_fluct_upper", float)
    set_if("PRICE_FLUCTUATION_LOWER", "price_fluct_lower", float)
    set_if("BRAND_ENHANCER", "brand_enhancer", lambda x: str(x).strip())
    set_if("MODEL_ENHANCER", "model_enhancer", lambda x: str(x).strip())
    set_if("SEASON", "season", lambda x: str(x).strip())
    set_if("RUN_MODE", "run_mode", lambda x: str(x).upper())
    set_if("TOTAL_OVERALL", "total_overall", int)
    set_if("BATCH_SIZE", "batch_size", int)
    set_if("PRIORITY_RUNLIST_GCS_URI", "priority_runlist_gcs_uri", str)
    
    if "LIMIT_TO_SEGMENTS" in overrides:
        val = overrides["LIMIT_TO_SEGMENTS"]
        if isinstance(val, list):
            conf.limit_segments = [s.strip() for s in val if s.strip()]
        elif isinstance(val, str):
            conf.limit_segments = [s.strip() for s in val.split(",") if s.strip()]
        logging.info(f"   🔹 Overriding limit_segments: {conf.limit_segments}")

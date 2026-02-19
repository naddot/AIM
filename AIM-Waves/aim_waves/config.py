import os
import secrets
import yaml
from dotenv import load_dotenv

# Load .env file if it exists
load_dotenv()

class Config:
    # Flask settings
    SECRET_KEY = os.environ.get("FLASK_SECRET_KEY", "dev-secret-key-1234")
    SESSION_COOKIE_SECURE = False
    SESSION_COOKIE_SAMESITE = 'Lax'
    SESSION_COOKIE_HTTPONLY = True 

    # Auth
    APP_ACCESS_PASSWORD = os.environ.get("APP_ACCESS_PASSWORD")

    # Business defaults
    DEFAULT_GOLDILOCKS_PCT = 15
    DEFAULT_PRICE_FLUCTUATION_UPPER = 1.1
    DEFAULT_PRICE_FLUCTUATION_LOWER = 0.9

    # Data paths 
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    CSV_PATH = os.environ.get("CSV_PATH", os.path.join(BASE_DIR, "data/resources/segmentlist.csv"))
    
    # GCP Project
    GCP_PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "bqsqltesting")

    # Load Model Config
    MODEL_CONFIG_PATH = os.path.join(BASE_DIR, "config/model_config.yaml")
    try:
        with open(MODEL_CONFIG_PATH, "r") as f:
            MODEL_CONFIG = yaml.safe_load(f)
    except Exception as e:
        print(f"⚠️ Failed to load model config from {MODEL_CONFIG_PATH}: {e}")
        MODEL_CONFIG = {}

    # Prompt Template Path
    PROMPT_TEMPLATE_DIR = os.path.join(BASE_DIR, "resources/prompts")
    GCS_BUCKET = "aim-home"

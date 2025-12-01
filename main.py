import os
import time
import json
import logging
import asyncio
import httpx
import pandas as pd
import math
import re
import datetime as dt
import tempfile
from typing import List
from bs4 import BeautifulSoup
from google.cloud import bigquery
from google.cloud import storage
from io import StringIO
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --------------------------------------------------------------------------------------
# CONFIG (override with env vars in Cloud Run Job)
# --------------------------------------------------------------------------------------

# Dry Run Mode: If True, skips all write operations (BQ, GCS)
DRY_RUN = os.getenv("DRY_RUN", "False").lower() in ("true", "1", "t")

PROJECT_ID = os.getenv("PROJECT_ID", "bqsqltesting")

# Stage 2 – TyreScore CSVs in GCS
TYRESCORE_BUCKET = os.getenv("TYRESCORE_BUCKET", "tyrescore")
TYRESCORE_PREFIX = os.getenv("TYRESCORE_PREFIX", "tyrescore-AWS3-daily-files/")
TYRESCORE_FILE_EXTENSION = os.getenv("TYRESCORE_FILE_EXTENSION", ".csv")

# Stage 4 – AIM batch runner config
AIM_BASE_URL = os.getenv(
    "AIM_BASE_URL",
    "https://tyrescore-waves-829092209663.europe-west1.run.app",
)
AIM_PAGE_SIZE = int(os.getenv("AIM_PAGE_SIZE", "45"))
AIM_TOTAL_PER_SEGMENT = int(os.getenv("AIM_TOTAL_PER_SEGMENT", "500"))
AIM_PARALLEL_SEGMENTS = int(os.getenv("AIM_PARALLEL_SEGMENTS", "7"))
AIM_REQUESTS_PER_SEGMENT = int(os.getenv("AIM_REQUESTS_PER_SEGMENT", "4"))
AIM_REQUEST_TIMEOUT_S = int(os.getenv("AIM_REQUEST_TIMEOUT_S", "900"))

AIM_GOLDILOCKS_ZONE_PCT = int(os.getenv("AIM_GOLDILOCKS_ZONE_PCT", "15"))
AIM_PRICE_FLUCT_UPPER = float(os.getenv("AIM_PRICE_FLUCT_UPPER", "1.1"))
AIM_PRICE_FLUCT_LOWER = float(os.getenv("AIM_PRICE_FLUCT_LOWER", "0.9"))
AIM_BRAND_ENHANCER = os.getenv("AIM_BRAND_ENHANCER", "").strip()
AIM_MODEL_ENHANCER = os.getenv("AIM_MODEL_ENHANCER", "").strip()
AIM_SEASON = os.getenv("AIM_SEASON", "").strip()  # "", "AllSeason", "Winter", "Summer"

# Comma-separated list of segment names or empty for all
AIM_LIMIT_SEGMENTS_ENV = os.getenv("AIM_LIMIT_SEGMENTS", "").strip()
if AIM_LIMIT_SEGMENTS_ENV:
    AIM_LIMIT_SEGMENTS: List[str] = [
        s.strip() for s in AIM_LIMIT_SEGMENTS_ENV.split(",") if s.strip()
    ]
else:
    AIM_LIMIT_SEGMENTS = []

AIM_SERVICE_PASSWORD = os.getenv("AIM_SERVICE_PASSWORD", "!BlU35qU4R3!")

# Stage 4 – GCS / BQ outputs
AIM_BUCKET_NAME = os.getenv("AIM_BUCKET_NAME", "aim-home")
AIM_GCS_PREFIX = os.getenv("AIM_GCS_PREFIX", "aim-daily-files")
AIM_DATASET_ID = os.getenv("AIM_DATASET_ID", "AIM")
AIM_TABLE_ID = os.getenv("AIM_TABLE_ID", "AIMData")
AIM_BQ_WRITE_DISPOSITION = os.getenv("AIM_BQ_WRITE_DISPOSITION", "WRITE_TRUNCATE")

# Optional: GCS URI for dynamic configuration
CONFIG_GCS_URI = os.getenv("CONFIG_GCS_URI")
IGNORE_GCS_CONFIG = os.getenv("IGNORE_GCS_CONFIG", "False").lower() in ("true", "1", "t")

def load_and_apply_gcs_config():
    """Attempts to load config from GCS and override global variables."""
    global AIM_TOTAL_PER_SEGMENT, AIM_GOLDILOCKS_ZONE_PCT
    global AIM_PRICE_FLUCT_UPPER, AIM_PRICE_FLUCT_LOWER
    global AIM_BRAND_ENHANCER, AIM_MODEL_ENHANCER, AIM_SEASON, AIM_LIMIT_SEGMENTS

    if IGNORE_GCS_CONFIG:
        logging.info("ℹ️ IGNORE_GCS_CONFIG is True. Skipping GCS config load.")
        return

    if not CONFIG_GCS_URI:
        logging.info("ℹ️ No CONFIG_GCS_URI set. Using environment variables.")
        return

    logging.info(f"⬇️ Attempting to download config from {CONFIG_GCS_URI}...")
    try:
        storage_client = storage.Client(project=PROJECT_ID)
        
        # Parse bucket and blob name from URI
        if not CONFIG_GCS_URI.startswith("gs://"):
            raise ValueError("CONFIG_GCS_URI must start with gs://")
        
        parts = CONFIG_GCS_URI[5:].split("/", 1)
        if len(parts) != 2:
            raise ValueError("Invalid GCS URI format")
            
        bucket_name, blob_name = parts
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        if not blob.exists():
            logging.warning(f"⚠️ Config file {CONFIG_GCS_URI} not found. Using defaults.")
            return

        config_str = blob.download_as_text()
        config = json.loads(config_str)
        logging.info("✅ Config downloaded and parsed successfully. Applying overrides...")

        # Helper to safely apply config
        def apply_val(key, var_name, cast_func):
            if key in config:
                try:
                    val = cast_func(config[key])
                    logging.info(f"   🔹 Overriding {var_name}: {val}")
                    return val
                except Exception as e:
                    logging.warning(f"   ⚠️ Invalid value for {key} in config: {config[key]} ({e}). Keeping default.")
            return None

        # Apply overrides with granular fallback
        val = apply_val("TOTAL_PER_SEGMENT", "AIM_TOTAL_PER_SEGMENT", int)
        if val is not None: AIM_TOTAL_PER_SEGMENT = val

        val = apply_val("GOLDILOCKS_ZONE_PCT", "AIM_GOLDILOCKS_ZONE_PCT", int)
        if val is not None: AIM_GOLDILOCKS_ZONE_PCT = val

        val = apply_val("PRICE_FLUCTUATION_UPPER", "AIM_PRICE_FLUCT_UPPER", float)
        if val is not None: AIM_PRICE_FLUCT_UPPER = val

        val = apply_val("PRICE_FLUCTUATION_LOWER", "AIM_PRICE_FLUCT_LOWER", float)
        if val is not None: AIM_PRICE_FLUCT_LOWER = val

        val = apply_val("BRAND_ENHANCER", "AIM_BRAND_ENHANCER", lambda x: str(x).strip())
        if val is not None: AIM_BRAND_ENHANCER = val

        val = apply_val("MODEL_ENHANCER", "AIM_MODEL_ENHANCER", lambda x: str(x).strip())
        if val is not None: AIM_MODEL_ENHANCER = val

        val = apply_val("SEASON", "AIM_SEASON", lambda x: str(x).strip())
        if val is not None: AIM_SEASON = val
        
        if "LIMIT_TO_SEGMENTS" in config:
            try:
                limit_to = config["LIMIT_TO_SEGMENTS"]
                if isinstance(limit_to, list):
                    AIM_LIMIT_SEGMENTS = [s.strip() for s in limit_to if s.strip()]
                elif isinstance(limit_to, str) and limit_to.strip():
                     AIM_LIMIT_SEGMENTS = [s.strip() for s in limit_to.split(",") if s.strip()]
                else:
                    AIM_LIMIT_SEGMENTS = []
                logging.info(f"   🔹 Overriding AIM_LIMIT_SEGMENTS: {AIM_LIMIT_SEGMENTS}")
            except Exception as e:
                logging.warning(f"   ⚠️ Invalid value for LIMIT_TO_SEGMENTS in config: {e}. Keeping default.")

        logging.info("✅ Configuration overrides applied.")

    except Exception as e:
        logging.error(f"❌ Failed to load/apply config from GCS: {e}. Using defaults.")

# Try to load GCS config immediately
load_and_apply_gcs_config()

def get_exact_file(gcs_client, base_name):
    """Checks if a file exists in GCS and returns its path."""
    bucket = gcs_client.bucket(TYRESCORE_BUCKET)
    blobs = list(bucket.list_blobs(prefix=TYRESCORE_PREFIX))
    
    # Look for file matching base_name + extension
    target_file = f"{base_name}{TYRESCORE_FILE_EXTENSION}"
    
    for blob in blobs:
        if blob.name.endswith(target_file):
             return blob.name
    return None

def run_stage_1():
    """Executes Stage 1: S3 (simulated) to GCS to BigQuery."""
    logging.info("Starting Stage 1...")
    
    bq_client = bigquery.Client(project=PROJECT_ID)
    gcs_client = storage.Client(project=PROJECT_ID)

    # -----------------------
    # Collect valid jobs
    # -----------------------
    data_jobs = []

    car_sales_path = get_exact_file(gcs_client, "CarMakeModelSales")
    if car_sales_path:
        data_jobs.append({
            "gcs_path": f"gs://{TYRESCORE_BUCKET}/{car_sales_path}",
            "bq_table": "nexus_tyrescore.CarMakeModelSales"
        })
    else:
        logging.warning("⚠️ Skipping CarMakeModelSales: file not found.")

    tyrescore_path = get_exact_file(gcs_client, "TyreScore")
    if tyrescore_path:
        data_jobs.append({
            "gcs_path": f"gs://{TYRESCORE_BUCKET}/{tyrescore_path}",
            "bq_table": "nexus_tyrescore.TyreScore"
        })
    else:
        logging.warning("⚠️ Skipping TyreScore: file not found.")

    # -----------------------
    # BigQuery Load Config
    # -----------------------
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Ignored for CarMakeModelSales because we manually define headers
    )

    # -----------------------
    # Process uploads
    # -----------------------
    for job in data_jobs:
        try:
            logging.info(f"📂 Processing {job['gcs_path']} → {PROJECT_ID}.{job['bq_table']}")

            if "CarMakeModelSales" in job['gcs_path']:
                # Headerless file — manually assign 9 expected columns
                expected_columns = [
                    "ProductId", "CarMake", "CarModel",
                    "Width", "Profile", "Rim",
                    "Orders", "Units", "AvgPrice"
                ]
                df = pd.read_csv(job['gcs_path'], header=None, names=expected_columns)
            else:
                # TyreScore file should have headers
                df = pd.read_csv(job['gcs_path'], header=0)

            # Clean column names for BigQuery compatibility
            df.columns = [
                str(col).strip().replace(' ', '_').replace('.', '_').replace('-', '_')
                for col in df.columns
            ]

            logging.info(f"🧪 Columns: {df.columns.tolist()}")

            # Load into BigQuery
            table_ref = f"{PROJECT_ID}.{job['bq_table']}"
            
            if not DRY_RUN:
                load_job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
                load_job.result()
                logging.info(f"✅ Uploaded {df.shape[0]} rows to {table_ref}")
            else:
                logging.info(f"🚧 DRY RUN: Would upload {df.shape[0]} rows to {table_ref}")

        except Exception as e:
            logging.error(f"❌ Failed to process {job['gcs_path']}: {e}")

def run_stage_3():
    """Executes Stage 3: TyreScore Algorithm (BigQuery SQL)."""
    logging.info("Starting Stage 3: TyreScore Algorithm...")
    
    bq_client = bigquery.Client(project=PROJECT_ID)
    
    # Read SQL from file
    sql_file_path = "tyrescore_algorithm.sql"
    try:
        with open(sql_file_path, "r") as f:
            query = f.read()
            
        logging.info(f"📜 Executing SQL from {sql_file_path}...")
        
        if not DRY_RUN:
            query_job = bq_client.query(query)
            query_job.result() # Wait for the job to complete
            logging.info("✅ Stage 3 completed successfully.")
        else:
            logging.info("🚧 DRY RUN: Would execute Stage 3 SQL.")
        
    except FileNotFoundError:
        logging.error(f"❌ SQL file not found: {sql_file_path}")
    except Exception as e:
        logging.error(f"❌ Failed to execute Stage 3: {e}")

# -----------------------
# Stage 4 Helper Functions
# -----------------------
def log(msg):
    logging.info(msg)

def _ensure_not_redirected_to_login(resp, context: str):
    """Catch auth failures where protected endpoints bounce to /login."""
    final_url = str(resp.url).lower()
    if "/login" in final_url:
        raise RuntimeError(f"{context} redirected to login (auth likely failed).")
    for h in resp.history:
        loc = h.headers.get("location", "").lower()
        if h.status_code in (301, 302, 303, 307, 308) and "login" in loc:
            raise RuntimeError(f"{context} redirected to login (auth likely failed).")

async def login(client: httpx.AsyncClient):
    # If your service needs a password/login:
    logging.info(f"🔑 Logging in to {AIM_BASE_URL}...")
    r = await client.post(
        f"{AIM_BASE_URL}/login",
        data={"password": AIM_SERVICE_PASSWORD},
        timeout=30,
    )
    r.raise_for_status()
    if not client.cookies:
        raise RuntimeError("Login did not set any cookies; check AIM_SERVICE_PASSWORD or the expected login payload.")
    logging.info("✅ Login successful and session cookie set.")

async def fetch_segments(client: httpx.AsyncClient):
    r = await client.get(f"{AIM_BASE_URL}/app", timeout=30)
    r.raise_for_status()
    _ensure_not_redirected_to_login(r, "Fetching /app for segments")
    soup = BeautifulSoup(r.text, "html.parser")
    sel = soup.select_one("select[name='segment']")
    if not sel:
        raise RuntimeError("Couldn't find segment dropdown on /app")
    segments = [
        o.get_text(strip=True)
        for o in sel.find_all("option")
        if o.get_text(strip=True) and not o.get_text(strip=True).startswith("--")
    ]
    if not segments:
        raise RuntimeError("No segments parsed from /app")
    log(f"✅ Fetched {len(segments)} segment(s) via /app dropdown.")
    return segments

async def fetch_page(client, segment, offset, retries=2):
    params = {
        "segment": segment,
        "top_n": AIM_PAGE_SIZE,
        "offset": offset,
        "goldilocks_zone_pct": AIM_GOLDILOCKS_ZONE_PCT,
        "price_fluctuation_upper": AIM_PRICE_FLUCT_UPPER,
        "price_fluctuation_lower": AIM_PRICE_FLUCT_LOWER,
        "brand_enhancer": AIM_BRAND_ENHANCER or None,
        "model_enhancer": AIM_MODEL_ENHANCER or None,
        "season": AIM_SEASON or None,
    }
    params = {k: v for k, v in params.items() if v is not None}

    for attempt in range(retries + 1):
        try:
            r = await client.get(
                f"{AIM_BASE_URL}/api/recommendations",
                params=params,
                timeout=AIM_REQUEST_TIMEOUT_S,
            )
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == retries:
                log(f"✖ page offset {offset} for '{segment}' failed: {e}")
                raise
            await asyncio.sleep(1.5 * (attempt + 1))  # small backoff

async def run_segment(client, segment):
    pages = math.ceil(AIM_TOTAL_PER_SEGMENT / AIM_PAGE_SIZE)
    offsets = [i * AIM_PAGE_SIZE for i in range(pages)]
    sem = asyncio.Semaphore(AIM_REQUESTS_PER_SEGMENT)

    async def guarded(offset):
        async with sem:
            return await fetch_page(client, segment, offset)

    log(f"→ {segment}: running {pages} page(s) of {AIM_PAGE_SIZE}")
    results = await asyncio.gather(*[guarded(o) for o in offsets], return_exceptions=True)

    flat, ok = [], 0
    for res in results:
        if isinstance(res, Exception):
            continue
        flat.extend(res)
        ok += sum(1 for row in res if row.get("success"))
    log(f"✓ {segment}: {len(flat)} rows, {ok} successful")
    return segment, flat

async def run_all_segments():
    async with httpx.AsyncClient(follow_redirects=True) as client:
        await login(client)
        segments = await fetch_segments(client)
        if not segments:
            raise RuntimeError("No segments returned; authentication or parsing likely failed.")
        
        if AIM_LIMIT_SEGMENTS:
            wanted = set(AIM_LIMIT_SEGMENTS)
            segments = [s for s in segments if s in wanted]

        seg_sem = asyncio.Semaphore(AIM_PARALLEL_SEGMENTS)
        async def run_guarded(seg):
            async with seg_sem:
                return await run_segment(client, seg)

        return await asyncio.gather(*[run_guarded(s) for s in segments])

def normalize_size(s: str) -> str:
    s = str(s or "")
    # ensure a space before R / ZR / VR, etc.
    s = re.sub(r'(?i)(?<=\d)([A-Z]{0,2})R(?=\d)', r' \1R', s)
    return re.sub(r'\s+', ' ', s).strip()

def repair_vehicle_size(row):
    SIZE_CORE_RE = re.compile(
        r'''(?ix)
        \b(
            \d{3}/\d{2}\s*[A-Z]{0,2}R\d{2}            # 205/70R15, 225/40 ZR18
          | \d{2}/\d{3,4}(?:\.\d{2})?\s*[A-Z]{0,2}R\d{2}  # 31/1050 R15, 31/10.50 R15
          | \d{1,2}\.\d{2}\s*[A-Z]{0,2}R\d{2}         # 7.50 R16, 10.50 R15
          | \d{1,2}x\d{2}\.\d{2}\s*[A-Z]{0,2}R\d{2}   # 31x10.50 R15
        )\b
        '''
    )
    v = str(row["Vehicle"] or "").strip()
    s = str(row["Size"] or "").strip()

    # If Size contains leading model text, move it into Vehicle
    m = SIZE_CORE_RE.search(s)
    if m:
        prefix = s[:m.start()].strip()
        core = m.group(1)
        s = core
        if prefix:
            v = f"{v} {prefix}".strip()
    else:
        # Otherwise, try to extract size from Vehicle
        vm = SIZE_CORE_RE.search(v)
        if vm:
            s = vm.group(1)
            v = (v[:vm.start()] + " " + v[vm.end():]).strip()

    # Tidy Vehicle: add space between letters and digits ("ROVER90" -> "ROVER 90")
    v = re.sub(r'(?<=[A-Za-z])(?=\d)', ' ', v)
    v = re.sub(r'\s+', ' ', v).strip()

    # Normalize size spacing ("205/70R15" -> "205/70 R15", "225/40ZR18" -> "225/40 ZR18")
    s = normalize_size(s)
    return pd.Series({"Vehicle": v, "Size": s})

def process_stage4_results(results):
    # Shape the CSV
    rows = []
    for segment, items in results:
        for it in items:
            rows.append({
                "Segment": segment,
                "Vehicle": it.get("Vehicle"),
                "Size": it.get("Size"),
                "HB1": it.get("HB1"),
                "HB2": it.get("HB2"),
                "HB3": it.get("HB3"),
                "HB4": it.get("HB4"),
                "SKUs": " ".join(it.get("SKUs", [])),
                "success": it.get("success", False),
            })

    df = pd.DataFrame(rows)
    if df.empty:
        logging.warning("⚠️ No results fetched in Stage 4.")
        return None

    SKU_COLS = [f"SKU{i}" for i in range(1, 17)]
    def explode_skus(s):
        parts = str(s or "").split()
        parts = parts[:16] + [""] * max(0, 16 - len(parts))
        return pd.Series(parts, index=SKU_COLS)

    sku_df = df["SKUs"].apply(explode_skus)
    out = pd.concat([df[["Vehicle", "Size", "HB1", "HB2", "HB3", "HB4"]], sku_df], axis=1)

    # Vehicle/Size repair
    out[["Vehicle", "Size"]] = out.apply(repair_vehicle_size, axis=1)

    # Keep only the desired columns
    out = out[["Vehicle","Size","HB1","HB2","HB3","HB4", *SKU_COLS]]

    # Replace duplicate SKUs
    def _replace_duplicate_skus_in_row(row):
        seen = set()
        replaced = 0
        for col in SKU_COLS:
            val = row[col]
            if pd.isna(val):
                continue
            s = str(val).strip()
            if not s or s == "-":
                continue
            if s in seen:
                row[col] = "-"   # squash duplicate to dash
                replaced += 1
            else:
                seen.add(s)
        row["_dup_replaced"] = replaced
        return row

    out = out.apply(_replace_duplicate_skus_in_row, axis=1)
    dup_cells_replaced = int(out["_dup_replaced"].sum())
    out = out.drop(columns=["_dup_replaced"])
    logging.info(f"🔁 Replaced {dup_cells_replaced} duplicate SKU cells with '-'.")

    # Drop rows containing 'FormatError'
    bad_mask = out.astype(str).apply(lambda col: col.str.contains(r'\bFormatError\b', na=False)).any(axis=1)
    dropped_bad = int(bad_mask.sum())
    out = out[~bad_mask].copy()
    logging.info(f"🚮 Skipping {dropped_bad} rows containing 'FormatError'.")

    # De-dup
    DEDUP_KEY_COLUMNS = ["Vehicle", "Size"]
    before = len(out)
    out = out.drop_duplicates(subset=DEDUP_KEY_COLUMNS, keep="first")
    logging.info(f"🧹 Removed {before - len(out)} duplicate rows on {DEDUP_KEY_COLUMNS}.")
    
    return out

def run_stage_4():
    """Executes Stage 4: Batch Runner and AIM Override SQL."""
    logging.info("Starting Stage 4: Batch Runner...")
    
    # 1. Run Async Batch Runner
    try:
        results = asyncio.run(run_all_segments())
    except Exception as e:
        logging.error(f"❌ Async batch runner failed: {e}")
        return

    # 2. Process Results
    out_df = process_stage4_results(results)
    if out_df is None:
        logging.warning("⚠️ Skipping upload/load for Stage 4 due to empty results.")
    else:
        # 3. Save locally and upload
        run_date = dt.datetime.utcnow().strftime("%Y%m%d")
        file_basename = f"aim_daily_runner_output_{run_date}.csv"
        local_csv = os.path.join(tempfile.gettempdir(), file_basename)
        out_df.to_csv(local_csv, index=False)
        logging.info(f"✅ Wrote {len(out_df)} rows to {local_csv}")

        storage_client = storage.Client(project=PROJECT_ID)
        bucket = storage_client.bucket(AIM_BUCKET_NAME)
        blob_path = f"{AIM_GCS_PREFIX}/{file_basename}"
        
        if not DRY_RUN:
            bucket.blob(blob_path).upload_from_filename(local_csv)
            logging.info(f"✅ Uploaded to gs://{AIM_BUCKET_NAME}/{blob_path}")
        else:
            logging.info(f"🚧 DRY RUN: Would upload to gs://{AIM_BUCKET_NAME}/{blob_path}")

        # 4. Load into BigQuery
        bq_client = bigquery.Client(project=PROJECT_ID)
        table_ref = f"{PROJECT_ID}.{AIM_DATASET_ID}.{AIM_TABLE_ID}"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            field_delimiter=",",
            write_disposition=getattr(bigquery.WriteDisposition, AIM_BQ_WRITE_DISPOSITION, bigquery.WriteDisposition.WRITE_TRUNCATE),
            autodetect=False,
            schema=[
                bigquery.SchemaField("Vehicle","STRING"),
                bigquery.SchemaField("Size","STRING"),
                bigquery.SchemaField("HB1","STRING"),
                bigquery.SchemaField("HB2","STRING"),
                bigquery.SchemaField("HB3","STRING"),
                bigquery.SchemaField("HB4","STRING"),
                *[bigquery.SchemaField(f"SKU{i}","STRING") for i in range(1,17)],
            ],
        )
        uri = f"gs://{AIM_BUCKET_NAME}/{blob_path}"
        
        if not DRY_RUN:
            load_job = bq_client.load_table_from_uri(uri, table_ref, job_config=job_config)
            load_job.result()
            logging.info(f"✅ Loaded into {table_ref} with {AIM_BQ_WRITE_DISPOSITION}.")
        else:
            logging.info(f"🚧 DRY RUN: Would load into {table_ref} from {uri}")

def run_stage_5():
    """Executes Stage 5: Dashboard Updater (SQL)."""
    logging.info("Starting Stage 5: Dashboard Updater...")
    
    bq_client = bigquery.Client(project=PROJECT_ID)
    sql_file_path = "aim_dashboard_update.sql"
    try:
        with open(sql_file_path, "r") as f:
            query = f.read()
            
        logging.info(f"📜 Executing SQL from {sql_file_path}...")
        
        if not DRY_RUN:
            query_job = bq_client.query(query)
            query_job.result() # Wait for the job to complete
            logging.info("✅ Stage 5 SQL completed successfully.")
        else:
            logging.info("🚧 DRY RUN: Would execute Stage 5 SQL.")
        
    except FileNotFoundError:
        logging.error(f"❌ SQL file not found: {sql_file_path}")
    except Exception as e:
        logging.error(f"❌ Failed to execute Stage 5 SQL: {e}")

def run_stage_6():
    """Executes Stage 6: Insights Updater (SQL)."""
    logging.info("Starting Stage 6: Insights Updater...")
    
    bq_client = bigquery.Client(project=PROJECT_ID)
    sql_file_path = "aim_insights_update.sql"
    try:
        with open(sql_file_path, "r") as f:
            query = f.read()
            
        logging.info(f"📜 Executing SQL from {sql_file_path}...")
        
        if not DRY_RUN:
            query_job = bq_client.query(query)
            query_job.result() # Wait for the job to complete
            logging.info("✅ Stage 6 SQL completed successfully.")
        else:
            logging.info("🚧 DRY RUN: Would execute Stage 6 SQL.")
        
    except FileNotFoundError:
        logging.error(f"❌ SQL file not found: {sql_file_path}")
    except Exception as e:
        logging.error(f"❌ Failed to execute Stage 6 SQL: {e}")

def run_stage_7():
    """Executes Stage 7: Analysis Updater (SQL)."""
    logging.info("Starting Stage 7: Analysis Updater...")
    
    bq_client = bigquery.Client(project=PROJECT_ID)
    sql_file_path = "aim_analysis_update.sql"
    try:
        with open(sql_file_path, "r") as f:
            query = f.read()
            
        logging.info(f"📜 Executing SQL from {sql_file_path}...")
        
        if not DRY_RUN:
            query_job = bq_client.query(query)
            query_job.result() # Wait for the job to complete
            logging.info("✅ Stage 7 SQL completed successfully.")
        else:
            logging.info("🚧 DRY RUN: Would execute Stage 7 SQL.")
        
    except FileNotFoundError:
        logging.error(f"❌ SQL file not found: {sql_file_path}")
    except Exception as e:
        logging.error(f"❌ Failed to execute Stage 7 SQL: {e}")

def run_stage_8():
    """Executes Stage 8: Merchandising Updater (SQL)."""
    logging.info("Starting Stage 8: Merchandising Updater...")
    
    bq_client = bigquery.Client(project=PROJECT_ID)
    sql_file_path = "aim_merchandising_update.sql"
    try:
        with open(sql_file_path, "r") as f:
            query = f.read()
            
        logging.info(f"📜 Executing SQL from {sql_file_path}...")
        
        if not DRY_RUN:
            query_job = bq_client.query(query)
            query_job.result() # Wait for the job to complete
            logging.info("✅ Stage 8 SQL completed successfully.")
        else:
            logging.info("🚧 DRY RUN: Would execute Stage 8 SQL.")
        
    except FileNotFoundError:
        logging.error(f"❌ SQL file not found: {sql_file_path}")
    except Exception as e:
        logging.error(f"❌ Failed to execute Stage 8 SQL: {e}")

def run_stage_9():
    """Executes Stage 9: Size File Updater (SQL)."""
    logging.info("Starting Stage 9: Size File Updater...")
    
    bq_client = bigquery.Client(project=PROJECT_ID)
    sql_file_path = "aim_size_file_update.sql"
    try:
        with open(sql_file_path, "r") as f:
            query = f.read()
            
        logging.info(f"📜 Executing SQL from {sql_file_path}...")
        
        if not DRY_RUN:
            query_job = bq_client.query(query)
            query_job.result() # Wait for the job to complete
            logging.info("✅ Stage 9 SQL completed successfully.")
        else:
            logging.info("🚧 DRY RUN: Would execute Stage 9 SQL.")
        
    except FileNotFoundError:
        logging.error(f"❌ SQL file not found: {sql_file_path}")
    except Exception as e:
        logging.error(f"❌ Failed to execute Stage 9 SQL: {e}")

if __name__ == "__main__":
    run_stage_1()
    run_stage_3()
    run_stage_4()
    run_stage_5()
    run_stage_6()
    run_stage_7()
    run_stage_8()
    run_stage_9()

import logging
import pandas as pd
from google.cloud import bigquery
from context import Context

# Logic for Stage 1: Load S3/GCS files to BQ
def run(ctx: Context):
    # KNOWN_MAKES acts as a side-output of Stage 1 to help parsing later.
    # In main.py it was a global. 
    # We should return it or attach it to Context?
    # Context is a dataclass. Let's return it and have main.py store it? 
    # Or cleaner: Context can have a 'shared_state' dict.
    # Let's add 'known_makes' to Context dynamically or just return it.
    
    logging.info("Starting Stage 1...")
    
    # We need to find specific files. IOBackend doesn't have "get_exact_file" logic 
    # which scanned for matching extensions. We replicate that here.
    
    # Logic: Look for "CarMakeModelSales" + extension
    # Using a dedicated backend for the Tyrescore bucket if in Cloud mode
    
    prefix = ctx.config.tyrescore_prefix
    
    # Determine which IO backend to use
    if ctx.config.aim_mode == "local":
        io_backend = ctx.io
    else:
        # In Cloud mode, we need to read from the specific Tyrescore bucket
        from file_io.gcs_backend import GCSBackend
        logging.info(f"🔌 Initializing GCSBackend for bucket: {ctx.config.tyrescore_bucket}")
        io_backend = GCSBackend(ctx.config.project_id, ctx.config.tyrescore_bucket)

    files = io_backend.list_files(prefix)
    
    # Helper to find file
    def find_file(base_name):
        target = f"{base_name}{ctx.config.tyrescore_file_extension}"
        for f in files:
            if f.endswith(target):
                # Return strict relative path for reading
                # If IOBackend requires prefix to join, use resolve_path.
                # list_files returns path relative to backend root/prefix?
                # GCSBackend implementation returns relative to ROOT_PREFIX.
                # So we can just use the name returned.
                return f
        return None

    data_jobs = []
    
    # Car Sales
    f_sales = find_file("CarMakeModelSales")
    if f_sales:
        # Original code reconstructed gs:// uri for BQ load.
        # But we want to use 'load_table_from_dataframe' often for consistency/cleaning.
        # Main.py read CSV into DF, cleaned columns, then uploaded.
        data_jobs.append({"file": f_sales, "bq_table": "nexus_tyrescore.CarMakeModelSales"})
    else:
        logging.warning("⚠️ Skipping CarMakeModelSales: file not found.")

    # Tyre Score
    f_score = find_file("TyreScore")
    if f_score:
        data_jobs.append({"file": f_score, "bq_table": "nexus_tyrescore.TyreScore"})
    else:
        logging.warning("⚠️ Skipping TyreScore: file not found.")

    # BQ Config
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1, 
    )

    known_makes = set()

    for job in data_jobs:
        try:
             path = job['file']
             table = job['bq_table']
             logging.info(f"📂 Processing {path} → {ctx.config.project_id}.{table}")
             
             # Read content via IO Backend
             content = io_backend.read_text(path)
             from io import StringIO
             
             if "CarMakeModelSales" in path:
                 cols = ["ProductId", "CarMake", "CarModel", "Width", "Profile", "Rim", "Orders", "Units", "AvgPrice"]
                 df = pd.read_csv(StringIO(content), header=None, names=cols)
             else:
                 df = pd.read_csv(StringIO(content), header=0)

             # Clean cols
             df.columns = [
                str(col).strip().replace(' ', '_').replace('.', '_').replace('-', '_')
                for col in df.columns
             ]
             
             # Capture Makes
             if "CarMake" in df.columns:
                 uniques = df["CarMake"].dropna().unique()
                 for mk in uniques:
                     known_makes.add(str(mk).strip().upper())
                 logging.info(f"✅ Captured {len(uniques)} unique Makes.")

             # Load to BQ
             # We use our bq wrapper
             from bq import load_table_from_dataframe
             table_ref = f"{ctx.config.project_id}.{table}"
             load_table_from_dataframe(ctx.bq, df, table_ref, job_config, ctx.config.dry_run)

        except Exception as e:
            logging.error(f"❌ Failed to process {job['file']}: {e}")

    return known_makes

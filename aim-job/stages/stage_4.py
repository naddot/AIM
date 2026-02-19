import asyncio
import logging
import math
import os
import tempfile
import json
import pandas as pd
import datetime as dt
from google.cloud import bigquery
from context import Context
from io_manager import load_priority_runlist
from stages.processing import process_stage4_results

def build_cam_sku_df_from_aim(aim_df: pd.DataFrame) -> pd.DataFrame:
    required = ["Vehicle", "Size"] + [f"HB{i}" for i in range(1, 5)] + [f"SKU{i}" for i in range(1, 21)]
    missing = [c for c in required if c not in aim_df.columns]
    if missing:
        raise ValueError(f"AIMData missing required columns for CAM_SKU staging: {missing}")

    cam = aim_df[required].copy()

    # Normalise vehicle/size whitespace
    cam["Vehicle"] = cam["Vehicle"].astype("string").str.replace(r"\s+", " ", regex=True).str.strip()
    cam["Size"] = cam["Size"].astype("string").str.replace(r"\s+", " ", regex=True).str.strip()

    # Normalise product IDs: blanks/'-'/FormatError -> None
    for c in [f"HB{i}" for i in range(1, 5)] + [f"SKU{i}" for i in range(1, 21)]:
        cam[c] = (
            cam[c]
            .astype("string")
            .str.strip()
            .replace({"": None, "-": None, "FormatError": None})
        )

    cam["last_modified"] = dt.datetime.now(dt.timezone.utc)
    return cam


async def run(ctx: Context, known_makes: set):
    logging.info(f"Starting Stage 4: Batch Runner (Mode: {ctx.config.run_mode})...")
    
    # Common HTTP Client for all calls
    headers = {}
    oidc_token = ctx.waves.get_id_token(ctx.config.aim_base_url) # or aim_waves_url? main.py used base_url for token.
    if oidc_token:
        headers["Authorization"] = f"Bearer {oidc_token}"

    import httpx
    async with httpx.AsyncClient(follow_redirects=True, headers=headers) as client:
        await ctx.waves.login(client)
        
        if ctx.config.run_mode == "GLOBAL":
             results = await run_global_mode(ctx, client)
        else:
             results = await run_per_segment_mode(ctx, client)

    # Process Results
    aim_df, _ = process_stage4_results(results, known_makes)

    if aim_df is None:
        logging.warning("⚠️ Skipping upload/load for Stage 4 due to empty results.")
        return

    write_aim_data(ctx, aim_df)

    # Build CAM_SKU from AIM output (not from parse_vehicle_split)
    cam_df = build_cam_sku_df_from_aim(aim_df)
    write_cam_sku(ctx, cam_df)



async def run_per_segment_mode(ctx: Context, client):
    # Legacy Mode Logic from main.py
    segments = await ctx.waves.fetch_segments(client)
    if not segments: raise RuntimeError("No segments returned")
    
    if ctx.config.limit_segments:
        wanted = set(ctx.config.limit_segments)
        segments = [s for s in segments if s in wanted]

    sem = asyncio.Semaphore(ctx.config.parallel_segments)
    
    async def run_guarded(seg):
        async with sem:
            # Replicate run_segment logic logic locally or inside WavesClient?
            # It was inside main.py as helper. 
            # Logic: Calc pages, gather fetch_page calls.
            pages = math.ceil(ctx.config.total_per_segment / ctx.config.page_size)
            offsets = [i * ctx.config.page_size for i in range(pages)]
            
            # Inner Semaphore for Requests per segment
            req_sem = asyncio.Semaphore(ctx.config.requests_per_segment)
            async def fetch_guarded(off):
                async with req_sem:
                    return await ctx.waves.fetch_page(client, seg, off)
            
            logging.info(f"→ {seg}: running {pages} page(s)")
            page_results = await asyncio.gather(*[fetch_guarded(o) for o in offsets], return_exceptions=True)
            
            flat, ok = [], 0
            for res in page_results:
                if isinstance(res, Exception): continue
                if isinstance(res, list):
                    flat.extend(res)
                    ok += sum(1 for row in res if row.get("success"))
            
            logging.info(f"✓ {seg}: {len(flat)} rows, {ok} successful")
            return seg, flat

    raw_results = await asyncio.gather(*[run_guarded(s) for s in segments])
    return raw_results # List of (segment, items)


import httpx

async def refresh_auth(ctx: Context, client: httpx.AsyncClient):
    """
    Refreshes the OIDC token and updates the client headers and session cookie.
    """
    logging.info("🔄 Token expired. Refreshing...")
    # 1. New OIDC Token
    new_oidc = ctx.waves.get_id_token(ctx.config.aim_base_url)
    if new_oidc:
         client.headers["Authorization"] = f"Bearer {new_oidc}"
    
    # 2. Re-login for session cookie
    await ctx.waves.login(client)



async def fetch_batch_with_retry(ctx: Context, client: httpx.AsyncClient, run_id: str, batch: list, max_retries: int = 3) -> dict:
    """
    Robust fetch with:
    1. Token Refresh on 401 (max 2 attempts)
    2. Exponential Backoff on 429/5xx (max_retries)
    """
    attempt = 0
    auth_refreshes = 0
    
    while attempt <= max_retries:
        try:
            return await ctx.waves.fetch_batch(client, run_id, batch, log_file_backend=ctx.io)
            
        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            
            # Case 1: 401 Unauthorized -> Refresh Token
            if code == 401:
                if auth_refreshes < 2:
                    logging.warning(f"   ⚠️ 401 Unauthorized. Refreshing Token (Attempt {auth_refreshes+1}/2)...")
                    try:
                        await refresh_auth(ctx, client)
                        auth_refreshes += 1
                        # Do NOT increment 'attempt' counter for auth issues, 
                        # so we still have full retries for other errors.
                        continue 
                    except Exception as auth_e:
                        logging.error(f"   ❌ Failed to refresh auth: {auth_e}")
                        raise # Fatal if we can't refresh
                else:
                    logging.error("   ❌ Max auth refreshes exceeded.")
                    raise

            # Case 2: 429/5xx -> Backoff and Retry
            if code == 429 or 500 <= code < 600:
                if attempt < max_retries:
                    delay = 2 ** attempt # 1s, 2s, 4s...
                    logging.warning(f"   ⚠️ HTTP {code} Error. Retrying in {delay}s (Attempt {attempt+1}/{max_retries})...")
                    await asyncio.sleep(delay)
                    attempt += 1
                    continue
                else:
                    raise # Max retries hit

            # Other Client Errors (400, 403, 404) -> Raise immediately
            raise
            
        except (httpx.RequestError, httpx.TimeoutException) as e:
            # Case 3: Network/Timeout -> Backoff and Retry
            if attempt < max_retries:
                delay = 2 ** attempt
                logging.warning(f"   ⚠️ Transport/Timeout Error: {e}. Retrying in {delay}s (Attempt {attempt+1}/{max_retries})...")
                await asyncio.sleep(delay)
                attempt += 1
                continue
            raise

    raise RuntimeError("Max retries exceeded unexpectedly")


async def run_global_mode(ctx: Context, client):
    ctx.tracker.update(state="running", last_log_line="Loading priority runlist...")
    run_df = load_priority_runlist(ctx.config, ctx.io)
    
    if run_df is None or run_df.empty:
        raise RuntimeError("Priority runlist is empty or failed to load.")

    run_df = run_df.head(ctx.config.total_overall)
    total_cams = len(run_df)
    logging.info(f"🚀 Starting GLOBAL mode for top {total_cams} CAMs (Batch size: {ctx.config.batch_size})")
    
    run_id = f"global_{datetime_now_str()}"
    all_cams = run_df[["Vehicle", "Size"]].to_dict("records")
    batches = [all_cams[i : i + ctx.config.batch_size] for i in range(0, total_cams, ctx.config.batch_size)]
    
    all_results = [None] * total_cams
    total_usage = {"prompt_token_count": 0, "candidates_token_count": 0, "total_token_count": 0}

    # Processing Loop
    for i, batch in enumerate(batches):
         logging.info(f"   📦 Processing batch {i+1}/{len(batches)}...")
         ctx.tracker.update(last_log_line=f"Processing batch {i+1}/{len(batches)}")
         
         start_idx = i * ctx.config.batch_size
         try:
             # Use ctx.io for logging if local mode
             batch_resp = await fetch_batch_with_retry(ctx, client, run_id, batch)
             
             # Aggregate results
             b_results = batch_resp.get("results", [])
             for j, res in enumerate(b_results):
                 all_results[start_idx + j] = res
             
             usage = batch_resp.get("usage", {})
             for k in total_usage: total_usage[k] += usage.get(k, 0)

         except Exception as e:
             logging.error(f"   ❌ Batch failed after retries: {e}")
             for j, cam in enumerate(batch):
                 all_results[start_idx + j] = {"Vehicle": cam["Vehicle"], "Size": cam["Size"], "success": False, "error_code": "BATCH_FAILED"}

         # Progress
         processed = min((i + 1) * ctx.config.batch_size, total_cams)
         succeeded = sum(1 for r in all_results[:processed] if r and r.get("success"))
         failed = sum(1 for r in all_results[:processed] if r and not r.get("success"))

         # Batch specific stats
         batch_results = all_results[start_idx : start_idx + len(batch)]
         batch_success = sum(1 for r in batch_results if r and r.get("success"))
         logging.info(f"   📊 Batch Result: {batch_success}/{len(batch)} CAMs succeeded.")
         
         if failed > 0:
             # Log first few errors to help debugging
             start_fail_chk = i * ctx.config.batch_size
             end_fail_chk = min((i + 1) * ctx.config.batch_size, total_cams)
             errors_logged = 0
             for k in range(start_fail_chk, end_fail_chk):
                 res = all_results[k]
                 if res and not res.get("success"):
                     err = res.get("error_code") or res.get("error_type") or "Unknown Error"
                     logging.warning(f"   ⚠️ Item {k} ({res.get('Vehicle', '?')}/{res.get('Size', '?')}) failed: {err}")
                     errors_logged += 1
                     if errors_logged >= 3: break # Don't spam
         
         ctx.tracker.update(progress={
             "attempted": processed,
             "succeeded": succeeded,
             "failed": failed
         })

    # Retry Logic (1 pass for individual failed items, distinct from batch retry)
    failed_indices = [i for i, r in enumerate(all_results) if not r or not r.get("success")]
    if failed_indices:
        logging.info(f"   🔄 Retrying {len(failed_indices)} failed CAMs (Individual Retry)...")
        failed_cams = [all_cams[i] for i in failed_indices]
        retry_batches = [failed_cams[i:i+ctx.config.batch_size] for i in range(0, len(failed_cams), ctx.config.batch_size)]
        
        for i, batch in enumerate(retry_batches):
            logging.info(f"   🔄 Retry Batch {i+1}/{len(retry_batches)} ({len(batch)} items)...")
            try:
                # Use the same robust fetch for retries
                batch_resp = await fetch_batch_with_retry(ctx, client, run_id + "_retry", batch)
                b_results = batch_resp.get("results", [])
                
                batch_start_failed = i * ctx.config.batch_size
                for j, res in enumerate(b_results):
                     # Aggregate usage
                     usage = batch_resp.get("usage", {})
                     for k in total_usage: total_usage[k] += usage.get(k, 0)
                     
                     if res.get("success"):
                         orig_idx = failed_indices[batch_start_failed + j]
                         all_results[orig_idx] = res
            
            except Exception as e:
                logging.error(f"   ❌ Retry batch {i+1} failed completely: {e}")

    # Final Report
    success_count = sum(1 for r in all_results if r and r.get("success"))
    generate_cost_report(ctx, total_usage, success_count, total_cams)
    
    # Format for processing: List of (mode, results_flat)
    return [("GLOBAL", all_results)]


def write_aim_data(ctx: Context, df):
    # Upload AIMData to GCS and Load to BQ
    run_id = ctx.tracker.run_id # Use tracker's ID
    basename = f"results_{run_id}.csv"
    
    # Save locally first (IOBackend)
    # If Cloud, we prefer tempfile but IOBackend is strict about paths relative to root.
    # We should use tempfile for intermediate CSV if we want to avoid polluting backend root?
    # Or just write to "output/" via backend.
    
    # Use output/ path in backend
    path = f"output/{basename}"
    ctx.io.write_text(path, df.to_csv(index=False))
    logging.info(f"✅ Wrote csv to {path}")
    ctx.tracker.update(output_file=basename)

    if ctx.config.aim_mode == "local":
        logging.info("ℹ️ Local Mode: Skipping BQ Load.")
        return

    # Cloud Mode: BQ Load
    # We need the GCS URI.
    # GCS Backend adds prefix automatically? No, resolve_path does.
    # We need to construct the gs:// URI for BQ.
    if ctx.config.aim_mode != "local":
         # Assume GCSBackend
         # We need to know the bucket and key.
         # Key inside backend is 'path' (resolved later by backend mechanics, but resolved path includes prefix?)
         # GCSBackend.resolve_path returns "root_prefix/path".
         object_name = ctx.io.resolve_path(path)
         uri = f"gs://{ctx.config.aim_bucket_name}/{object_name}"
         
         # Load BQ
         table_ref = f"{ctx.config.project_id}.{ctx.config.aim_dataset_id}.{ctx.config.aim_table_id}"
         
         from bq import load_table_from_uri
         # Job Config...
         j_conf = bigquery.LoadJobConfig(
              source_format=bigquery.SourceFormat.CSV,
              skip_leading_rows=1,
              write_disposition=getattr(bigquery.WriteDisposition, ctx.config.bq_write_disposition, 'WRITE_TRUNCATE'),
              autodetect=False,
              # Schema hardcoded in main.py, replica here?
              schema=[
                  bigquery.SchemaField("Vehicle","STRING"),
                  bigquery.SchemaField("Size","STRING"),
                  bigquery.SchemaField("HB1","STRING"),
                  bigquery.SchemaField("HB2","STRING"),
                  bigquery.SchemaField("HB3","STRING"),
                  bigquery.SchemaField("HB4","STRING"),
                  *[bigquery.SchemaField(f"SKU{i}","STRING") for i in range(1,21)],
              ]
         )
         load_table_from_uri(ctx.bq, uri, table_ref, j_conf, ctx.config.dry_run)


def write_cam_sku(ctx: Context, df: pd.DataFrame):
    """
    Upload CAM_SKU staging data to a fixed schema staging table,
    then run MERGE into CAM_SKU using aim_cam_sku_update.sql.
    """
    run_id = ctx.tracker.run_id
    basename = f"cam_sku_{run_id}.csv"
    path = f"output/{basename}"
    ctx.io.write_text(path, df.to_csv(index=False))
    logging.info(f"✅ Wrote CAM_SKU CSV to {path}")

    if ctx.config.aim_mode == "local":
        return

    # Target and fixed staging
    cam_table_id = ctx.config.cam_table_id  # e.g. bqsqltesting.CAM_files.CAM_SKU

    # IMPORTANT: set this to the table you created
    staging_id = getattr(ctx.config, "cam_sku_staging_table_id", None) or "bqsqltesting.CAM_files.CAM_SKU_staging"

    from bq import load_table_from_dataframe, execute_query

    # Force schema (NO autodetect)
    schema = [
        bigquery.SchemaField("Vehicle", "STRING"),
        bigquery.SchemaField("Size", "STRING"),
        *[bigquery.SchemaField(f"HB{i}", "STRING") for i in range(1, 5)],
        *[bigquery.SchemaField(f"SKU{i}", "STRING") for i in range(1, 21)],
        bigquery.SchemaField("last_modified", "TIMESTAMP"),
    ]

    df = df.copy()
    df["Vehicle"] = df["Vehicle"].astype("string").str.strip()
    df["Size"] = df["Size"].astype("string").str.strip()

    for i in range(1, 5):
        c = f"HB{i}"
        if c not in df.columns:
            df[c] = None
        df[c] = (
            df[c].astype("string").str.strip()
            .replace({"": None, "-": None, "FormatError": None})
        )


    for i in range(1, 21):
        c = f"SKU{i}"
        if c not in df.columns:
            df[c] = None
        df[c] = (
            df[c]
            .astype("string")
            .str.strip()
            .replace({"": None, "-": None, "FormatError": None})
        )
    if "last_modified" not in df.columns:
        df["last_modified"] = dt.datetime.now(dt.timezone.utc)

    # 1) Load staging table (truncate each run)
    j_conf = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=False,
        schema=schema
    )
    load_table_from_dataframe(ctx.bq, df, staging_id, j_conf, ctx.config.dry_run)

    # 2) Execute MERGE (staging -> target)
    # The SQL file is in the root (parent of stages/)
    sql_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "aim_cam_sku_update.sql")
    with open(sql_path, "r", encoding="utf-8") as f:
        template = f.read()


    sql = (
        template
        .replace("{cam_table_id}", cam_table_id)
        .replace("{staging_table_id}", staging_id)
    )
    execute_query(ctx.bq, sql, ctx.config.dry_run)
    logging.info("✅ CAM_SKU merge executed successfully.")


def datetime_now_str():
    return dt.datetime.now().strftime("%Y%m%d_%H%M%S")

def generate_cost_report(ctx: Context, total_usage: dict, success: int, total: int):
    """
    Calculates cost based on Gemini 2.5 Flash-Lite pricing and records it.
    Input: £0.072505 / 1M tokens, Output: £0.29002 / 1M tokens
    """
    input_price = 0.072505 / 1_000_000
    output_price = 0.29002 / 1_000_000
    
    input_tokens = total_usage.get("prompt_token_count", 0)
    output_tokens = total_usage.get("candidates_token_count", 0)
    
    total_cost = (input_tokens * input_price) + (output_tokens * output_price)
    
    report = {
        "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
        "run_id": ctx.tracker.run_id,
        "mode": ctx.config.run_mode,
        "units": {
            "cams_attempted": total,
            "cams_succeeded": success,
        },
        "usage": total_usage,
        "estimated_cost_gbp": round(total_cost, 5)
    }
    
    # Log to console
    logging.info("=" * 40)
    logging.info("📊 STAGE 4 COST REPORT")
    logging.info(f"   Tokens: {input_tokens:,} in / {output_tokens:,} out")
    logging.info(f"   Cost:   £{total_cost:.5f}")
    logging.info(f"   Success: {success}/{total}")
    logging.info("=" * 40)
    
    # Save via IOBackend
    ctx.io.write_text("output/cost_report.json", json.dumps(report, indent=2))
    
    # Update Status Tracker
    ctx.tracker.update(report=report)

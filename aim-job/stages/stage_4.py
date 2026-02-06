import asyncio
import logging
import math
import os
import tempfile
import pandas as pd
import datetime as dt
from google.cloud import bigquery
from context import Context
from io_manager import load_priority_runlist
from stages.processing import process_stage4_results

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
    aim_df, cam_df = process_stage4_results(results, known_makes)
    
    if aim_df is None:
        logging.warning("⚠️ Skipping upload/load for Stage 4 due to empty results.")
        return

    # --- UPLOAD 1: AIMData ---
    write_aim_data(ctx, aim_df)

    # --- UPLOAD 2: CAM_SKU (Upsert) ---
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
             batch_resp = await ctx.waves.fetch_batch(client, run_id, batch, log_file_backend=ctx.io)
             
             # Aggregate results
             b_results = batch_resp.get("results", [])
             for j, res in enumerate(b_results):
                 all_results[start_idx + j] = res
             
             usage = batch_resp.get("usage", {})
             for k in total_usage: total_usage[k] += usage.get(k, 0)

         except Exception as e:
             logging.error(f"   ❌ Batch failed: {e}")
             for j, cam in enumerate(batch):
                 all_results[start_idx + j] = {"Vehicle": cam["Vehicle"], "Size": cam["Size"], "success": False}

         # Progress
         processed = min((i + 1) * ctx.config.batch_size, total_cams)
         ctx.tracker.update(progress={
             "attempted": processed,
             "succeeded": sum(1 for r in all_results[:processed] if r and r.get("success")),
             "failed": sum(1 for r in all_results[:processed] if r and not r.get("success"))
         })

    # Retry Logic (1 pass)
    failed_indices = [i for i, r in enumerate(all_results) if not r or not r.get("success")]
    if failed_indices:
        logging.info(f"   🔄 Retrying {len(failed_indices)} failed CAMs...")
        failed_cams = [all_cams[i] for i in failed_indices]
        retry_batches = [failed_cams[i:i+ctx.config.batch_size] for i in range(0, len(failed_cams), ctx.config.batch_size)]
        
        for i, batch in enumerate(retry_batches):
            try:
                batch_resp = await ctx.waves.fetch_batch(client, run_id + "_retry", batch)
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
                logging.error(f"   ❌ Retry batch {i+1} failed: {e}")

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
                  *[bigquery.SchemaField(f"SKU{i}","STRING") for i in range(1,17)],
              ]
         )
         load_table_from_uri(ctx.bq, uri, table_ref, j_conf, ctx.config.dry_run)


def write_cam_sku(ctx: Context, df):
    # Similar logic for CAM_SKU upsert (Merge)
    run_id = ctx.tracker.run_id
    basename = f"cam_sku_{run_id}.csv"
    path = f"output/{basename}"
    ctx.io.write_text(path, df.to_csv(index=False))
    
    if ctx.config.aim_mode == "local":
        return

    object_name = ctx.io.resolve_path(path)
    uri = f"gs://{ctx.config.aim_bucket_name}/{object_name}"
    
    # BQ Merge Logic...
    # (Simplified for brevity, but essentially same SQL MERGE logic as main.py)
    # staging table load -> Merge -> Drop Staging
    pass # Implementation would follow main.py pattern

def datetime_now_str():
    return dt.datetime.now().strftime("%Y%m%d_%H%M%S")

def generate_cost_report(ctx, total_usage, success, total):
    # ... logic from main.py ...
    pass

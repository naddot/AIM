from __future__ import annotations

import hashlib
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from google.cloud import bigquery

from aim_waves.config import Config

logger = logging.getLogger(__name__)

# ---- Config ----
BQ_TABLE = "bqsqltesting.nexus_tyrescore.TyreScore_algorithm_output"
BQ_LIMIT = 100

CSV_CANDIDATES = (
    "benchmark_final_balanced.csv",
    "aim_waves/benchmark_final_balanced.csv",
    "../benchmark_final_balanced.csv",
)

CACHE_DIRNAME = "cache"
CACHE_PREFIX = "tyre_data_"
CACHE_SUFFIX = ".json"


def _normalise_size(size: Optional[str]) -> str:
    """Normalise a tyre size for matching/caching (case-insensitive, no spaces)."""
    return (size or "").strip().lower().replace(" ", "")



def _normalise_vehicle(vehicle: Optional[str]) -> str:
    """Normalise a vehicle string (upper, no spaces, no special chars)."""
    if not vehicle:
        return ""
    # Remove all non-alphanumeric chars
    return "".join(c for c in vehicle if c.isalnum()).lower()


def _cache_path_for_query(size: Optional[str], vehicle: Optional[str]) -> Path:
    """Deterministic cache file path for size + vehicle combo."""
    base_dir = Path(__file__).resolve().parent / CACHE_DIRNAME
    base_dir.mkdir(parents=True, exist_ok=True)
    
    s_key = _normalise_size(size) or "any_size"
    v_key = _normalise_vehicle(vehicle) or "any_vehicle"
    
    raw_key = f"{s_key}_{v_key}"
    digest = hashlib.md5(raw_key.encode("utf-8")).hexdigest()
    return base_dir / f"{CACHE_PREFIX}{digest}{CACHE_SUFFIX}"


def _load_from_cache(cache_file: Path) -> Optional[List[Dict[str, Any]]]:
    if not cache_file.exists():
        return None
    try:
        with cache_file.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        logger.warning("⚠️ Cache file has unexpected format (not a list): %s", cache_file)
        return None
    except Exception as e:
        logger.warning("⚠️ Failed to read cache file %s: %s", cache_file, e)
        return None


def _save_to_cache(cache_file: Path, rows: List[Dict[str, Any]]) -> None:
    try:
        with cache_file.open("w", encoding="utf-8") as f:
            json.dump(rows, f, default=str, indent=2)
    except Exception as e:
        logger.warning("⚠️ Failed to write cache file %s: %s", cache_file, e)


def _fetch_from_bigquery(size: Optional[str], vehicle: Optional[str]) -> List[Dict[str, Any]]:
    """Fetch feedback rows from BigQuery for a given size AND vehicle."""
    size_norm = _normalise_size(size)
    vehicle_norm = _normalise_vehicle(vehicle)
    
    if not size_norm:
        return []

    client = bigquery.Client(project=Config.GCP_PROJECT)

    # Base query
    query = f"""
        SELECT *
        FROM `{BQ_TABLE}`
        WHERE LOWER(REPLACE(SIZE, ' ', '')) LIKE @SIZE_PATTERN
    """
    
    query_params = [
        bigquery.ScalarQueryParameter("SIZE_PATTERN", "STRING", f"%{size_norm}%"),
    ]

    # Add Vehicle Filter if present
    if vehicle_norm:
         # Match strictly (or LIKE if preferred, but usually vehicle is specific)
         # Using flexible match: REPLACE(UPPER(Vehicle), ' ', '') = vehicle_norm
         # But in BQ regex might be safer to strip all special chars.
         # For now, let's use a simple REPLACE logic similar to Python normalization
         # Assuming Vehicle column exists and is populated
         query += """
            AND UPPER(REGEXP_REPLACE(Vehicle, r'[^a-zA-Z0-9]', '')) = @VEHICLE_NORM
         """
         query_params.append(
             bigquery.ScalarQueryParameter("VEHICLE_NORM", "STRING", vehicle_norm)
         )

    query += f"""
        ORDER BY TyreScore ASC, Units DESC
        LIMIT {BQ_LIMIT}
    """

    job_config = bigquery.QueryJobConfig(query_parameters=query_params)

    try:
        query_job = client.query(query, job_config=job_config)
        return [dict(row) for row in query_job.result()]
    except Exception as e:
        logger.error(f"❌ BigQuery Error: {e}")
        # Build independent fallback logic or return empty to trigger CSV fallback
        return []


def _fetch_from_csv(size: Optional[str], vehicle: Optional[str]) -> List[Dict[str, Any]]:
    """Fallback to local CSV if BigQuery is unavailable."""
    csv_path = _find_csv_path()
    if not csv_path:
        return []

    size_norm = _normalise_size(size)
    vehicle_norm = _normalise_vehicle(vehicle)
    
    if not size_norm:
        return []

    try:
        df = pd.read_csv(csv_path)

        if "SIZE" not in df.columns:
            return []

        # Size Filter
        mask = df["SIZE"].astype(str).str.replace(" ", "", regex=False).str.lower().str.contains(size_norm)
        
        # Vehicle Filter (if provided and column exists)
        if vehicle_norm and "Vehicle" in df.columns:
             v_mask = df["Vehicle"].astype(str).str.replace(r'[^a-zA-Z0-9]', '', regex=True).str.upper() == vehicle_norm
             mask = mask & v_mask

        filtered = df[mask].copy()

        if filtered.empty:
            logger.warning("⚠️ Local fallback: no tyres found for size %s (veh: %s)", size, vehicle)
            return []

        return filtered.to_dict("records")
    except Exception as e:
        logger.error("❌ Local fallback failed: %s", e)
        return []


def fetch_feedback_from_bigquery(size: Optional[str], vehicle: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Fetch recommendation candidate rows for a given tyre size AND vehicle.
    """
    # New vehicle-aware cache key
    cache_file = _cache_path_for_query(size, vehicle)

    cached = _load_from_cache(cache_file)
    if cached is not None:
        return cached

    # Try BQ
    rows = _fetch_from_bigquery(size, vehicle)
    if rows:
        logger.info(f"✅ Found {len(rows)} rows for Vehicle '{vehicle}' + Size '{size}'")
    
    # If BQ returns empty BUT we had a vehicle filter, maybe try falling back to JUST size?
    # For now, let's trust the filter. If no specific vehicle data, maybe we want generic data?
    # User said "add vehicle back", implies strict filtering.
    # But if 0 rows, we might want generic.
    # Let's add a quick fallback to generic size-only if vehicle-specific search fails
    if not rows and vehicle:
        logger.info(f"ℹ️ No specific data for vehicle {vehicle}, falling back to size-only search.")
        rows = _fetch_from_bigquery(size, None)
        if rows:
            logger.info(f"✅ Found {len(rows)} rows for Size '{size}' (Fallback)")

    if not rows:
         # Try CSV as last resort (with fallback logic handled inside or here? 
         # _fetch_from_csv also has vehicle logic now.
         # Let's try explicit CSV fetch
         rows = _fetch_from_csv(size, vehicle)
         if rows:
             logger.info(f"✅ Found {len(rows)} rows from CSV for Vehicle '{vehicle}' + Size '{size}'")
         
         if not rows and vehicle:
             rows = _fetch_from_csv(size, None)
             if rows:
                 logger.info(f"✅ Found {len(rows)} rows from CSV for Size '{size}' (Fallback)")

    if rows:
        _save_to_cache(cache_file, rows)
    
    return rows


def fetch_feedback_batch(sizes: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Fetch feedback for multiple sizes in a single BigQuery call.
    Returns a dictionary mapping normalized_size -> list of rows.
    """
    if not sizes:
        return {}
        
    # Deduplicate and normalise
    norm_map = {} # norm -> original
    unique_norms = set()
    
    for s in sizes:
        n = _normalise_size(s)
        if n:
            unique_norms.add(n)
            norm_map[n] = s
            
    if not unique_norms:
        return {}
        
    # Check cache first for each size? 
    # For now, let's keep it simple and just hit BQ for bulk to avoid complex partial caching logic.
    # Or we could check cache and only query missing? 
    # Given the goal is speed and bulk, let's query BQ. Caching individual files is fine for single access.
    # Actually, we can just query BQ.
    
    client = bigquery.Client(project=Config.GCP_PROJECT)
    
    # Construct WHERE IN clause
    # BQ supports parameterized arrays? Yes, but safer to build dynamic SQL for clean string matching if needed.
    # simpler: WHERE LOWER(REPLACE(SIZE, ' ', '')) IN UNNEST(@sizes)
    
    query = f"""
        SELECT *
        FROM `{BQ_TABLE}`
        WHERE LOWER(REPLACE(SIZE, ' ', '')) IN UNNEST(@size_list)
        ORDER BY TyreScore ASC, Units DESC
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("size_list", "STRING", list(unique_norms))
        ]
    )
    
    results_map = {n: [] for n in unique_norms}
    
    try:
        query_job = client.query(query, job_config=job_config)
        rows = [dict(row) for row in query_job.result()]
        
        # Group by size
        for row in rows:
            # We need to match row back to normalized size
            # Since we selected * we might need to normalize the row's size
            row_size = str(row.get("SIZE", "") or "")
            n_row = _normalise_size(row_size)
            if n_row in results_map:
                results_map[n_row].append(row)
                
        logger.info(f"✅ Bulk Fetch: Retrieved {len(rows)} rows for {len(unique_norms)} sizes.")
        return results_map
        
    except Exception as e:
        logger.error(f"❌ Bulk BulkQuery Error: {e}")
        return {}

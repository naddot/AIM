import pandas as pd
import logging
import datetime as dt
from stages.sizes import repair_vehicle_size, parse_vehicle_split, parse_size_split

def process_stage4_results(results, known_makes: set):
    """
    Pure logic function to process API results into DataFrames.
    Returns (aim_df, cam_df).
    """
    rows = []
    # results is list of (segment_id, items) or (mode, items)
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
        return None, None

    # SUPPORT UP TO 24 SKUS NOW
    SKU_COLS_24 = [f"SKU{i}" for i in range(1, 25)]
    def explode_skus(s):
        parts = str(s or "").split()
        parts = parts[:24] + [""] * max(0, 24 - len(parts))
        return pd.Series(parts, index=SKU_COLS_24)

    sku_df = df["SKUs"].apply(explode_skus)
    out = pd.concat([df[["Vehicle", "Size", "HB1", "HB2", "HB3", "HB4"]], sku_df], axis=1)

    # Vehicle/Size repair
    out[["Vehicle", "Size"]] = out.apply(repair_vehicle_size, axis=1)

    # Replace duplicate SKUs
    def _replace_duplicate_skus_in_row(row):
        seen = set()
        replaced = 0
        for col in SKU_COLS_24:
            val = row[col]
            if pd.isna(val): continue
            s = str(val).strip()
            if not s or s == "-": continue
            if s in seen:
                row[col] = "-"
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
    bad_mask = out.astype(str).apply(lambda col: col.str.contains(r'FormatError', na=False)).any(axis=1)
    dropped_bad = int(bad_mask.sum())
    out = out[~bad_mask].copy()
    logging.info(f"🚮 Skipping {dropped_bad} rows containing 'FormatError'.")

    # De-dup on Vehicle/Size
    DEDUP_KEY_COLUMNS = ["Vehicle", "Size"]
    before = len(out)
    out = out.drop_duplicates(subset=DEDUP_KEY_COLUMNS, keep="first")
    logging.info(f"🧹 Removed {before - len(out)} duplicate rows on {DEDUP_KEY_COLUMNS}.")

    # --- PREPARE DATASET 1: AIMData ---
    aim_cols = ["Vehicle","Size","HB1","HB2","HB3","HB4"] + [f"SKU{i}" for i in range(1, 17)]
    aim_df = out[aim_cols].copy()

    # --- PREPARE DATASET 2: CAM_SKU ---
    cam_rows = []
    # Force UTC aware for production/correctness
    # But main.py legacy used: dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f UTC")
    timestamp = dt.datetime.now(dt.timezone.utc).isoformat()
    
    for idx, row in out.iterrows():
        make, model = parse_vehicle_split(row["Vehicle"], known_makes)
        w, p, r = parse_size_split(row["Size"])
        
        new_row = {
            "Vehicle": row["Vehicle"],
            "Size": row["Size"],
            "Make": make,
            "Model": model,
            "Width": w,
            "Profile": p,
            "Rim": r,
            "last_modified": timestamp
        }
        for i in range(1, 25):
            val = row.get(f"SKU{i}")
            s_val = str(val) if val is not None else ""
            if s_val == "-" or s_val.lower() == "nan": s_val = ""
            if s_val.endswith(".0"): s_val = s_val[:-2] # pandas float/int artifact
            s_val = s_val.strip()

            if len(s_val) == 8:
                 new_row[f"SKU{i}"] = s_val
            else:
                 new_row[f"SKU{i}"] = None
            
        cam_rows.append(new_row)
        
    cam_df = pd.DataFrame(cam_rows)
    return aim_df, cam_df

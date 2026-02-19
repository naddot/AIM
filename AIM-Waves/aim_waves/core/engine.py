import logging
import httpx
import os
import json
import concurrent.futures
from datetime import datetime
from google import genai
from google.genai import types
from google.api_core.exceptions import GoogleAPIError

from aim_waves.config import Config
from aim_waves.core.utils import normalize_string_for_comparison, robust_parse_output, parse_recommendation_output
from aim_waves.core.prompts import get_error_output, construct_prompt

from aim_waves.data.bigquery import fetch_feedback_from_bigquery, fetch_feedback_batch, _normalise_size, _normalise_vehicle
from aim_waves.data.loader import vehicle_batch_map

logger = logging.getLogger(__name__)

# Track startup for status API
START_TIME = datetime.now()



import time

def process_single_cam(cam, params, prefetched_data=None):
    """Worker function for batch processing a single Vehicle/Size combination."""
    veh = cam.get("Vehicle")
    sz = cam.get("Size")
    
    if not veh or not sz or str(veh).lower() == "nan" or str(sz).lower() == "nan":
        return {
            "Vehicle": veh or "Unknown",
            "Size": sz or "Unknown",
            "success": False,
            "error_code": "INVALID_INPUT"
        }

    try:
        # Call with return_metadata=True to get usage stats even on success
        res_data = generate_recommendation(
            vehicle=veh,
            size=sz,
            goldilocks_zone_pct=params.get("goldilocks_zone_pct", 15),
            price_fluctuation_upper=params.get("price_fluctuation_upper", 1.1),
            price_fluctuation_lower=params.get("price_fluctuation_lower", 0.9),
            brand_enhancer=params.get("brand_enhancer"),
            model_enhancer=params.get("model_enhancer"),
            seasonal_performance=params.get("season"),
            pod_filter=params.get("pod"),
            segment_filter=params.get("segment"),
            disable_search=params.get("disable_search", True), # Default to True for cost/speed in batch

            return_metadata=True,
            prefetched_data=prefetched_data
        )

        raw_result = res_data["output"]
        feedback_data = res_data.get("feedback_data", [])
        usage = res_data.get("usage", {})
        veh_out, size_out, hb1, hb2, hb3, hb4, skus = parse_recommendation_output(raw_result)
        
        # --- BACKFILL LOGIC ---
        # Ensure we have a full set of 20 unique SKUs (4 HB + 16 SKU)
        # 1. Gather Used IDs
        slots = [hb1, hb2, hb3, hb4] + skus
        used_ids = set()
        clean_slots = []
        
        for s in slots:
            s_str = str(s).strip()
            # Keep if valid digit and not duplicate
            # Keep if valid digit and length 7 or 8 and not duplicate
            if s_str.isdigit() and len(s_str) in (7, 8) and s_str not in used_ids:
                clean_slots.append(s_str)
                used_ids.add(s_str)
            else:
                clean_slots.append(None) # Mark for fill
        
        # 2. Prepare Candidates (ordered by relevance/popularity from BQ)
        candidates = []
        for row in feedback_data:
            pid = str(row.get('ProductId', ''))
            if pid and pid.isdigit() and len(pid) in (7, 8):
                candidates.append(pid)
        
        # 3. Fill Gaps
        final_ids = []
        cand_idx = 0
        
        for slot in clean_slots:
            if slot:
                final_ids.append(slot)
            else:
                # Find next unused candidate
                filled = False
                while cand_idx < len(candidates):
                    c = candidates[cand_idx]
                    cand_idx += 1
                    if c not in used_ids:
                        final_ids.append(c)
                        used_ids.add(c)
                        filled = True
                        break
                if not filled:
                   final_ids.append("-") # Truly out of stock

        # 4. Re-assign
        if len(final_ids) >= 4:
            hb1, hb2, hb3, hb4 = final_ids[0], final_ids[1], final_ids[2], final_ids[3]
            skus = final_ids[4:] if len(final_ids) > 4 else []
        
        # Pad SKUs to 16 if needed (though loop above should handle 24 slots if clean_slots was 24)
        # Ensure skus has 20 items
        while len(skus) < 20:
            skus.append("-")
            
        # Specific check for NoDataError from BigQuery
        if "NoDataError" in str(hb1) or "NoDataError" in raw_result:
             return {
                "Vehicle": veh, "Size": sz,
                "HB1": "Error", "HB2": "Error", "HB3": "Error", "HB4": "Error",
                "SKUs": ["-"] * 20,
                "success": False, "error_code": "NO_RESULTS",
                "usage": usage
            }

        is_success = (hb1.isdigit() and len(hb1) in (7, 8)) and \
                     (hb2.isdigit() and len(hb2) in (7, 8)) and \
                     (hb3.isdigit() and len(hb3) in (7, 8)) and \
                     (hb4.isdigit() and len(hb4) in (7, 8))

        if not is_success:
            logger.warning(f"Batch attempt 1 failed for {veh}/{sz}. Retrying...")
            res_data = generate_recommendation(
                vehicle=veh,
                size=sz,
                goldilocks_zone_pct=params.get("goldilocks_zone_pct", 15),
                price_fluctuation_upper=params.get("price_fluctuation_upper", 1.1),
                price_fluctuation_lower=params.get("price_fluctuation_lower", 0.9),
                brand_enhancer=params.get("brand_enhancer"),
                model_enhancer=params.get("model_enhancer"),
                seasonal_performance=params.get("season"),
                return_metadata=True
            )
            raw_result = res_data["output"]
            feedback_data = res_data.get("feedback_data", []) # Update feedback data? Usually same.
            
            # Combine usage from both attempts
            new_usage = res_data.get("usage", {})
            for k in usage:
                usage[k] = (usage.get(k) or 0) + (new_usage.get(k) or 0)

            veh_out, size_out, hb1, hb2, hb3, hb4, skus = parse_recommendation_output(raw_result)
            
            # --- BACKFILL LOGIC (RETRY) ---
            # Repeat backfill for retry result
            slots = [hb1, hb2, hb3, hb4] + skus
            used_ids = set()
            clean_slots = []
            for s in slots:
                s_str = str(s).strip()
                if s_str.isdigit() and len(s_str) in (7, 8) and s_str not in used_ids:
                    clean_slots.append(s_str)
                    used_ids.add(s_str)
                else:
                    clean_slots.append(None)
            
            # Candidates again
            candidates = []
            for row in feedback_data:
                pid = str(row.get('ProductId', ''))
                if pid and pid.isdigit() and len(pid) in (7, 8):
                    candidates.append(pid)
                    
            final_ids = []
            cand_idx = 0
            for slot in clean_slots:
                if slot:
                    final_ids.append(slot)
                else:
                    filled = False
                    while cand_idx < len(candidates):
                        c = candidates[cand_idx]
                        cand_idx += 1
                        if c not in used_ids:
                            final_ids.append(c)
                            used_ids.add(c)
                            filled = True
                            break
                    if not filled: final_ids.append("-")

            if len(final_ids) >= 4:
                hb1, hb2, hb3, hb4 = final_ids[0], final_ids[1], final_ids[2], final_ids[3]
                skus = final_ids[4:] if len(final_ids) > 4 else []
            while len(skus) < 20: skus.append("-")

            is_success = (hb1.isdigit() and len(hb1) in (7, 8)) and \
                         (hb2.isdigit() and len(hb2) in (7, 8)) and \
                         (hb3.isdigit() and len(hb3) in (7, 8)) and \
                         (hb4.isdigit() and len(hb4) in (7, 8))

        return {
            "Vehicle": veh,
            "Size": sz,
            "HB1": hb1,
            "HB2": hb2,
            "HB3": hb3,
            "HB4": hb4,
            "SKUs": skus,
            "success": is_success,
            "error_code": None if is_success else "UPSTREAM_ERROR",
            "usage": usage
        }
    except Exception as e:
        logger.error(f"❌ Batch error for {veh}/{sz}: {e}")
        err_msg = str(e).upper()
        code = "INTERNAL_ERROR"
        if "TIMEOUT" in err_msg: code = "TIMEOUT"
        elif "API" in err_msg: code = "UPSTREAM_ERROR"
        
        return {
            "Vehicle": veh,
            "Size": sz,
            "HB1": "Error", "HB2": "Error", "HB3": "Error", "HB4": "Error",
            "SKUs": ["-"] * 20,
            "success": False,
            "error_code": code
        }

def generate_recommendations_batch_push(run_id, cams, params):
    """
    New Batch Push Engine.
    Processes a list of CAMs in parallel (5 at a time).
    Preserves input order.
    """
    results = [None] * len(cams)
    
    # Limit: 30s per task, 120s total batch
    BATCH_TIMEOUT = 120
    CAM_TIMEOUT = 30

    batch_usage = {
        "prompt_token_count": 0,
        "candidates_token_count": 0,
        "total_token_count": 0
    }

    # API Limit / Concurrency Control
    # Default to 10 to be safe with Flash-Lite quotas, was 25
    max_workers = int(os.environ.get("AIM_MAX_WORKERS", "10"))

    # 1. Bulk Fetch Data from BigQuery
    unique_sizes = list({cam.get("Size") for cam in cams if cam.get("Size")})
    prefetched_data = fetch_feedback_batch(unique_sizes)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_index = {
            executor.submit(process_single_cam, cam, params, prefetched_data): i 
            for i, cam in enumerate(cams)
        }
        
        # Wait with total timeout
        done, not_done = concurrent.futures.wait(
            future_to_index.keys(), 
            timeout=BATCH_TIMEOUT
        )
        
        for future in done:
            idx = future_to_index[future]
            try:
                res = future.result()
                results[idx] = res
                
                # Aggregate usage
                cam_usage = res.get("usage", {})
                for k in batch_usage:
                    batch_usage[k] += cam_usage.get(k, 0)
                    
            except Exception as e:
                logger.error(f"CAM error at index {idx}: {e}")
                results[idx] = {
                    "Vehicle": cams[idx].get("Vehicle", "Unknown"),
                    "Size": cams[idx].get("Size", "Unknown"),
                    "success": False, "error_code": "INTERNAL_ERROR"
                }

        for future in not_done:
            idx = future_to_index[future]
            logger.error(f"TIMEOUT for CAM at index {idx}")
            results[idx] = {
            "Vehicle": cams[idx].get("Vehicle", "Unknown"),
            "Size": cams[idx].get("Size", "Unknown"),
            "HB1": "Error", "HB2": "Error", "HB3": "Error", "HB4": "Error",
            "SKUs": ["-"] * 20,
            "success": False,
            "error_code": "TIMEOUT",
            "usage": {}
        }
            future.cancel()

    return {
        "run_id": run_id,
        "results": results,
        "usage": batch_usage
    }

import time

def generate_recommendation(vehicle, size,
                             goldilocks_zone_pct=15, price_fluctuation_upper=1.1, price_fluctuation_lower=0.9,
                             brand_enhancer="Anybrand", model_enhancer="Anymodel",
                             seasonal_performance=None, pod_filter=None, segment_filter=None,
                             override_model=None, disable_search=False,

                             thinking_budget=None, stream=True, benchmark_mode=False, return_metadata=False,
                             prefetched_data=None):
    
    t_start = time.time()
    
    # Parameters normalization
    brand_enhancer_lower = (brand_enhancer or "anybrand").strip().lower()
    model_enhancer_lower = (model_enhancer or "anymodel").strip().lower()

    # Input validation / clamping
    if not (5 <= goldilocks_zone_pct <= 50):
        goldilocks_zone_pct = 15
    if not (1.0 <= price_fluctuation_upper <= 2.0):
        price_fluctuation_upper = 1.1
    if not (0.5 <= price_fluctuation_lower <= 1.0):
        price_fluctuation_lower = 0.9

    # Call Gemini using Dynamic Config
    model_cfg = Config.MODEL_CONFIG.get('model', {})
    search_cfg = Config.MODEL_CONFIG.get('vertex_ai_search', {})
    
    # Apply Overrides
    current_model_name = override_model if override_model else model_cfg.get('name', 'gemini-2.5-flash-lite')

    # Keys for caching
    safe_vehicle_key = normalize_string_for_comparison(vehicle).replace(" ", "_").replace("-", "_")
    safe_size_key = normalize_string_for_comparison(size).replace(" ", "_").replace("/", "_").replace("-", "_")
    safe_brand_key = brand_enhancer_lower.replace(" ", "_").replace("/", "-")
    safe_model_key = model_enhancer_lower.replace(" ", "_").replace("/", "-")
    safe_pod_key = (pod_filter or "AnyPod").strip().lower().replace(" ", "_")
    safe_seg_key = (segment_filter or "AnySegment").strip().lower().replace(" ", "_")
    safe_season_key = (seasonal_performance or "AnySeason").strip().lower()

    key = (
        f"{safe_vehicle_key}_{safe_size_key}"
        f"_g{int(goldilocks_zone_pct)}"
        f"_u{str(price_fluctuation_upper).replace('.', '_')}"
        f"_l{str(price_fluctuation_lower).replace('.', '_')}"
        f"_b{safe_brand_key}_m{safe_model_key}"
        f"_pod{safe_pod_key}_seg{safe_seg_key}_season{safe_season_key}"
    )

    if not benchmark_mode:
        pass # Cache removed per user request

    # 3. Fetch Data
    feedback_data = []
    
    if prefetched_data:
        # Optimisation: Use bulk-fetched data from memory
        n_size = _normalise_size(size)
        size_rows = prefetched_data.get(n_size, [])
        
        # Filter by Vehicle (replicating BQ logic)
        n_veh = _normalise_vehicle(vehicle)
        if n_veh:
            feedback_data = [r for r in size_rows if _normalise_vehicle(r.get("Vehicle")) == n_veh]
            
        # Fallback: If no vehicle specific data, use all data for size (Generic)
        if not feedback_data:
            feedback_data = size_rows
            
    # If pre-fetch missed (or wasn't provided), standard fetch (cache -> BQ -> CSV)
    if not feedback_data:
        feedback_data = fetch_feedback_from_bigquery(size, vehicle)

    if not feedback_data:
        # logger.warning(f"❌ No feedback data for {vehicle} {size}.")
        if return_metadata:
            return {
                "output": get_error_output(vehicle, size, "NoDataError"),
                "success": False,
                "error_type": "NoDataError",
                "model": current_model_name,
                "search_enabled": False,
                "thinking_budget": thinking_budget,
                "latency_ms": 0,
                "total_ms": int((time.time() - t_start) * 1000),
                "usage": {},
                "feedback_data": [] # Return empty list
            }
        return get_error_output(vehicle, size, "NoDataError")

    # Format data string
    # Format data as CSV (Pipe Separated) to save tokens
    # Using more descriptive headers to ensure model understands the columns
    headers = [
        "TyreScore", "ProdID", "WetGrade", "Brand", "Model", "WetVal", "FuelVal", "NoiseVal", 
        "Season", "IsOE", "AwardScore", "IsRunflat", "Segment", "PriceScore", "WetScore", "FuelScore", 
        "WetScorePct", "AwardScorePct", "Vehicle", "Size", "PriceGBP", "IsOffer", "PriceFluct", 
        "Orders", "Units", "Goldilocks", "PremShare", "MidShare", "BudShare", "RFShare", 
        "Status", "Views", "ClickRate"
    ]
    
    rows = []
    rows.append("|".join(headers))
    
    for item in feedback_data:
        row = [
            str(item.get('TyreScore', '')),
            str(item.get('ProductId', '')),
            str(item.get('GRADE', '')),
            str(item.get('BRAND', '')),
            str(item.get('Model', '')),
            str(item.get('WET_GRIP', '')),
            str(item.get('FUEL', '')),
            str(item.get('NOISE_REDUCTION', '')),
            str(item.get('SEASONAL_PERFORMANCE', '')),
            str(item.get('OE', '')),
            str(item.get('AWARD_SCORE', '')),
            str(item.get('RunflatStatus', '')),
            str(item.get('Segment', '')),
            str(item.get('PRICE_pct', '')),
            str(item.get('GRADE_pct', '')),
            str(item.get('FUEL_pct', '')),
            str(item.get('WET_GRIP_pct', '')),
            str(item.get('AWARD_SCORE_pct', '')),
            str(item.get('Vehicle', '')),
            str(item.get('SIZE', '')),
            str(item.get('PRICE', '')),
            str(item.get('OFFER', '')),
            str(item.get('PRICEFLUCTUATION', '')),
            str(item.get('Orders', '')),
            str(item.get('Units', '')),
            str(item.get('GoldilocksZone', '')),
            str(item.get('PremiumShare', '')),
            str(item.get('MidRangeShare', '')),
            str(item.get('BudgetShare', '')),
            str(item.get('RunflatShare', '')),
            str(item.get('SalesStatus', '')),
            str(item.get('PRODUCTLISTVIEWS', '')),
            str(item.get('CLICKSTREAMRATE', ''))
        ]
        # Clean pipes from content to avoid breaking CSV
        clean_row = [c.replace("|", "/") for c in row]
        rows.append("|".join(clean_row))

    tyre_data_str = "\n".join(rows)
    
    # DEBUG: Log Reference SKUs (Order in prompt) for benchmarking
    # Skip header row [0]
    ref_skus = [r.split('|')[1] for r in rows[1:] if len(r.split('|')) > 1]
    # print(f"DEBUG_REF_SKUS: {json.dumps(ref_skus)}")

    brand_enhancer_text = ""
    if brand_enhancer_lower != "anybrand":
        brand_enhancer_text = (
            f"- Because the brand {brand_enhancer_lower} is currently on offer, customers are significantly more likely "
            f"to purchase these products, even if they fall outside the Goldilocks Zone or price fluctuation ranges.\n"
            f"- You must always include at least one tyre from the brand {brand_enhancer_lower} in the final Tyre Suggestions section, even if it has never sold to a {vehicle}.\n"
            f"- Select the {brand_enhancer_lower} model that is most similar to the most popular product for {vehicle} in {size} - you are permitted to override all other rules to ensure its inclusion.\n"
            f"- This is a hard rule: if no {brand_enhancer_lower} tyre appears in the recommendations, your output is invalid."
        )

    model_enhancer_text = ""
    if model_enhancer_lower != "anymodel":
        model_enhancer_text = (
            f"- Because the model {model_enhancer_lower} is currently being promoted, it must be included in the final Tyre Suggestions.\n"
            f"- You must select an exact match for {model_enhancer_lower} from the available data. Do NOT use any earlier, later, or similar versions of this model.\n"
            f"- This is a hard rule: if no {model_enhancer_lower} model appears in the recommendations, your output is invalid.\n"
            f"- IMPORTANT: When you include a tyre with the {model_enhancer_lower} model, it must always appear as **HB3** in the final output. Place it in the third hotbox position, even if its score is higher than the other tyres."
        )

    season_enhancer_text = ""
    _seasonal_val = (seasonal_performance or "").strip().lower()
    if _seasonal_val in {"summer", "winter", "allseason"}:
        season_enhancer_text = (
            f"- The customer has explicitly requested tyres designed for **{_seasonal_val}** use.\n"
            f"- You must select at least 1 tyre with Seasonal Performance marked as **{_seasonal_val.capitalize()}** within primary recommendations, subject to Slot Eligibility and the Non-Override Guardrails.\n"
            f"- If a Season enhancer product is chosen and it is Budget, it may only occupy HB4 (and only if BudgetShare permits). Otherwise use the top-scoring non-Budget seasonal tyre.\n"
            f"- IMPORTANT: Place the selected seasonal tyre in HB4 unless that would violate Budget placement/count; if so, place it in the highest eligible HB slot (HB3 if Budget; HB1/HB2 only if non-Budget).\n"
            f"- This is a hard rule: if no eligible **{_seasonal_val}** tyre appears in primary recommendations, your output is invalid."
        )

    text_input = construct_prompt(
        vehicle, size, tyre_data_str, 
        brand_enhancer_text, model_enhancer_lower, model_enhancer_text, 
        seasonal_performance, season_enhancer_text,
        goldilocks_zone_pct, price_fluctuation_upper, price_fluctuation_lower
    )

    # Log Prompt Stats
    # Log Prompt Stats
    char_count = len(text_input)
    est_tokens = char_count // 4
    logger.info(f"📝 Generated Prompt: {char_count:,} chars (~{est_tokens:,} tokens)")
    logger.info(f"📝 Feedback Data Rows: {len(feedback_data)}")

    # Call Gemini using Dynamic Config
    model_cfg = Config.MODEL_CONFIG.get('model', {})
    search_cfg = Config.MODEL_CONFIG.get('vertex_ai_search', {})
    
    # Apply Overrides
    current_model_name = override_model if override_model else model_cfg.get('name', 'gemini-2.5-flash-lite')
    
    # Use ADC
    project_id = model_cfg.get('project') or os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCLOUD_PROJECT")
    if not project_id:
        raise ValueError("Project ID not found in config or environment variables (GOOGLE_CLOUD_PROJECT).")

    client = genai.Client(
        vertexai=True,
        project=project_id,
        location=model_cfg.get('location', 'europe-west1') 
    )
    
    contents = [types.Content(role="user", parts=[types.Part(text=text_input)])]
    
    tools = []
    logger.info(f"🔍 DEBUG: disable_search={disable_search}, type={type(disable_search)}")
    if not disable_search and search_cfg.get('datastore_id'):
         logger.info(f"🔍 DEBUG: Enabling Datastore Tool: {search_cfg['datastore_id']}")
         tools = [
            types.Tool(retrieval=types.Retrieval(vertex_ai_search=types.VertexAISearch(datastore=search_cfg['datastore_id'])))
         ]
    else:
         logger.info("🔍 DEBUG: Tools are disabled (Search disabled or no datastore_id).")

    
    safety_settings = [
        types.SafetySetting(category=cat, threshold=thresh)
        for cat, thresh in model_cfg.get('safety_settings', {}).items()
    ]

    generation_config_args = {
        "max_output_tokens": model_cfg.get('parameters', {}).get('max_output_tokens', 8292),
        "safety_settings": safety_settings,
        "tools": tools,
        "temperature": model_cfg.get('parameters', {}).get('temperature', 0.5),
        "top_p": model_cfg.get('parameters', {}).get('top_p', 0.95),
    }

    if benchmark_mode:
        generation_config_args["temperature"] = 0.0
        generation_config_args["top_p"] = 1.0
        
    if thinking_budget and thinking_budget > 0:
        generation_config_args["thinking_config"] = types.ThinkingConfig(thinking_budget=thinking_budget)

    config = types.GenerateContentConfig(**generation_config_args)

    usage_metadata = {}
    t_model_start = time.time()
    t_model_end = 0

    full_response_text = ""
    error_type = None

    # Retry Logic for 429 Resource Exhausted
    max_retries = 3
    base_delay = 2
    
    for attempt in range(max_retries + 1):
        try:
            # logger.debug("⚡️ Sending prompt to Gemini...")
            if stream:
                response_chunks = []
                for chunk in client.models.generate_content_stream(
                    model=current_model_name,
                    contents=contents,
                    config=config
                ):
                    if chunk.candidates and chunk.candidates[0].content and chunk.candidates[0].content.parts:
                        part_text = chunk.candidates[0].content.parts[0].text
                        response_chunks.append(part_text)
                    
                    # Capture usage from stream (usually in the last chunk)
                    if chunk.usage_metadata:
                        usage_metadata = {
                            "prompt_token_count": chunk.usage_metadata.prompt_token_count,
                            "candidates_token_count": chunk.usage_metadata.candidates_token_count,
                            "total_token_count": chunk.usage_metadata.total_token_count
                        }
                
                t_model_end = time.time()
                full_response_text = "".join(response_chunks)
            
            else:
                response = client.models.generate_content(
                    model=current_model_name,
                    contents=contents,
                    config=config
                )
                t_model_end = time.time()
                
                if response.candidates and response.candidates[0].content and response.candidates[0].content.parts:
                        full_response_text = response.candidates[0].content.parts[0].text
                
                if response.usage_metadata:
                    usage_metadata = {
                        "prompt_token_count": response.usage_metadata.prompt_token_count,
                        "candidates_token_count": response.usage_metadata.candidates_token_count,
                        "total_token_count": response.usage_metadata.total_token_count
                    }
            
            # If successful, break retry loop
            error_type = None
            break

        except (GoogleAPIError, httpx.RequestError, httpx.TimeoutException) as e:
            # Check for 429 specifically
            is_429 = False
            error_str = str(e)
            if "429" in error_str or "RESOURCE_EXHAUSTED" in error_str:
                is_429 = True
            
            if is_429 and attempt < max_retries:
                delay = base_delay * (2 ** attempt)
                logger.warning(f"⚠️ Quota exceeded (429). Retrying in {delay}s... (Attempt {attempt+1}/{max_retries})")
                time.sleep(delay)
                continue
            
            logger.error(f"❌ Gemini API error: {repr(e)}")
            error_type = "APIError"
            full_response_text = ""
            break
            
        except Exception as e:
            logger.error(f"❌ Unexpected error during Gemini generation: {repr(e)}")
            error_type = "StreamError" if stream else "GenerationError"
            full_response_text = ""
            break
    
    t_end = time.time()

    if error_type:
        if return_metadata:
             return {
                "output": "",
                "success": False,
                "error_type": error_type,
                "model": current_model_name,
                "search_enabled": bool(tools),
                "thinking_budget": thinking_budget,
                "latency_ms": int((t_model_end - t_model_start) * 1000) if t_model_end > 0 else 0,
                "total_ms": int((t_end - t_start) * 1000),
                "usage": usage_metadata
            }
        return get_error_output(vehicle, size, error_type)

    generated_text = full_response_text
    
    # DEBUG: Log raw output to understand refusal/format issues
    # print(f"DEBUG_RAW_OUTPUT:\n{generated_text}\n-------------------")

    if not generated_text.strip():
        logger.warning("⚠️ Gemini returned no content.")
        if return_metadata:
             return {
                "output": get_error_output(vehicle, size, "NoContent"),
                "success": False,
                "error_type": "NoContent",
                "model": current_model_name,
                "search_enabled": bool(tools),
                "thinking_budget": thinking_budget,
                "latency_ms": int((t_model_end - t_model_start) * 1000),
                "total_ms": int((t_end - t_start) * 1000),
                "usage": usage_metadata
            }
        return get_error_output(vehicle, size, "NoContent")

    final_output_string = ""
    success = False

    for line in generated_text.strip().splitlines():
        tokens = line.strip().split()
        n = len(tokens)

        if n < 6:
            continue

        for v_end in range(1, n - 4):
            for s_len in (1, 2, 3):
                after = v_end + s_len
                if after > n:
                    continue

                vehicle_candidate = ' '.join(tokens[:v_end])
                size_candidate = ' '.join(tokens[v_end:after])
                product_ids = tokens[after:]

                norm_vc = normalize_string_for_comparison(vehicle_candidate)
                norm_v = normalize_string_for_comparison(vehicle)
                norm_sc = normalize_string_for_comparison(size_candidate)
                norm_s = normalize_string_for_comparison(size)

                if norm_vc == norm_v and norm_sc == norm_s:
                    padded_ids = (product_ids + ['-'] * (24 - len(product_ids)))[:24]
                    hotboxes = padded_ids[:4]
                    skus = padded_ids[4:]

                    if not all(pid.isdigit() or pid == '-' for pid in hotboxes + skus):
                        continue

                    final_output_string = f"{vehicle_candidate} {size_candidate} {' '.join(hotboxes)} {' '.join(skus)}"

                    success = True
                    break
            if success:
                break
        if success:
            break

    # Robust Fallback
    if not success:
        robust_result = robust_parse_output(generated_text, vehicle, size)
        if robust_result:
            final_output_string = robust_result
            success = True
            logger.info(f"✅ Robust parser saved the day for {vehicle} {size}!")


    if return_metadata:
         return {
            "output": final_output_string if success else generated_text,
            "success": success,
            "error_type": None if success else "FormatError",
            "model": current_model_name,
            "search_enabled": bool(tools),
            "thinking_budget": thinking_budget,
            "latency_ms": int((t_model_end - t_model_start) * 1000),
            "total_ms": int((t_end - t_start) * 1000),
            "usage": usage_metadata,
            "feedback_data": feedback_data
        }
            
    if success:
        return final_output_string

    if return_metadata:
         return {
            "output": get_error_output(vehicle, size, "FormatError"),
            "success": False,
            "error_type": "FormatError",
            "model": current_model_name,
            "search_enabled": bool(tools),
            "thinking_budget": thinking_budget,
            "latency_ms": int((t_model_end - t_model_start) * 1000),
            "total_ms": int((t_end - t_start) * 1000),
            "usage": usage_metadata
        }
    return get_error_output(vehicle, size, "FormatError")

def generate_batch_recommendations(top_n=5, goldilocks_zone_pct=15,
                                 price_fluctuation_upper=1.1,
                                 price_fluctuation_lower=0.9,
                                 brand_enhancer=None,
                                 model_enhancer=None,
                                 pod_filter=None,
                                 segment_filter=None,
                                 seasonal_performance=None,
                                 offset=0):

    results = []
    filtered_entries = [
        ((veh, sz), meta)
        for (veh, sz), meta in vehicle_batch_map.items()
        if (not pod_filter or meta.get("pod", "").lower() == pod_filter.lower()) and
           (not segment_filter or meta.get("segment", "").lower() == segment_filter.lower())
    ]
    start = max(0, int(offset))
    stop = start + int(top_n)
    top_entries = filtered_entries[start:stop]

    for (veh, sz), _ in top_entries:
        try:
            # logger.debug(f"Calling generate_recommendation for {veh}/{sz} (Attempt 1)")
            raw_result = generate_recommendation(
                vehicle=veh,
                size=sz,
                goldilocks_zone_pct=goldilocks_zone_pct,
                price_fluctuation_upper=price_fluctuation_upper,
                price_fluctuation_lower=price_fluctuation_lower,
                brand_enhancer=brand_enhancer,
                model_enhancer=model_enhancer,
                seasonal_performance=seasonal_performance,
                pod_filter=pod_filter,
                segment_filter=segment_filter
            )

            veh_out, size_out, hb1, hb2, hb3, hb4, skus = parse_recommendation_output(raw_result)
            is_success = hb1.isdigit() and hb2.isdigit() and hb3.isdigit() and hb4.isdigit()

            if not is_success:
                logger.warning(f"Attempt 1 failed for {veh}/{sz} (Output: {raw_result}). Retrying...")
                raw_result = generate_recommendation(
                    vehicle=veh,
                    size=sz,
                    goldilocks_zone_pct=goldilocks_zone_pct,
                    price_fluctuation_upper=price_fluctuation_upper,
                    price_fluctuation_lower=price_fluctuation_lower,
                    brand_enhancer=brand_enhancer,
                    model_enhancer=model_enhancer,
                    seasonal_performance=seasonal_performance,
                    pod_filter=pod_filter,
                    segment_filter=segment_filter
                )
                veh_out, size_out, hb1, hb2, hb3, hb4, skus = parse_recommendation_output(raw_result)
                is_success = hb1.isdigit() and hb2.isdigit() and hb3.isdigit() and hb4.isdigit()
                if not is_success:
                    logger.error(f"Attempt 2 failed for {veh}/{sz} (Output: {raw_result}). Final status: FAILED.")

            results.append({
                "Vehicle": veh_out,
                "Size": size_out,
                "HB1": hb1,
                "HB2": hb2,
                "HB3": hb3,
                "HB4": hb4,
                "SKUs": skus,
                "success": is_success
            })
        except Exception as e:
            logger.error(f"❌ Error during recommendation generation for {veh}/{sz}: {e}", exc_info=True)
            results.append({
                "Vehicle": veh,
                "Size": sz,
                "HB1": "-", "HB2": "-", "HB3": "-", "HB4": "-",
                "SKUs": ["-"] * 20,
                "success": False
            })
    return results

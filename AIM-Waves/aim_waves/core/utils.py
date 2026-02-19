import re
import logging

logger = logging.getLogger(__name__)

def simple_normalize_text(text):
    if text is None:
        return ""
    return re.sub(r'[\s/-]+', '', text).upper()

def normalize_string_for_comparison(s):
    if s is None:
        return ""
    s = s.lower()
    s = re.sub(r'[^a-z0-9]', '', s)
    return s

def robust_parse_output(raw_text, vehicle, size):
    """
    Smarter, more forgiving parser that extracts Product IDs (8+ digits) 
    and verifies that the requested Vehicle and Size are mentioned.
    """
    if not raw_text:
        return None 

    norm_v = normalize_string_for_comparison(vehicle)
    norm_s = normalize_string_for_comparison(size)
    norm_text = normalize_string_for_comparison(raw_text)
    
    if norm_v not in norm_text or norm_s not in norm_text:
        logger.debug(f"Robust parse failed: Target {norm_v} or {norm_s} not found in normalized text.")
        return None

    clean_text = re.sub(r'[^a-zA-Z0-9\s-]', ' ', raw_text)
    tokens = clean_text.split()

    product_ids = []
    for t in tokens:
        if t.isdigit() and len(t) in (7, 8):
            product_ids.append(t)
        elif t == '-' and len(product_ids) > 0:
            product_ids.append(t)

    if len(product_ids) < 4:
        return None

    clean_v = vehicle.strip().upper().replace(" ", "_")
    clean_s = size.strip().upper().replace("/", "").replace(" ", "")
    
    # Support up to 24 products if available (HB1-4 + SKU1-20)
    padded_ids = (product_ids + ["-"] * 24)[:24]
    
    return f"{clean_v} {clean_s} {' '.join(padded_ids)}"

def parse_recommendation_output(raw_text):
    if not raw_text:
        return "ERROR_VEHICLE", "ERROR_SIZE", "Error", "Error", "Error", "Error", ["Error"] * 20

    tokens = raw_text.strip().split()
    
    if len(tokens) < 6:
        return "ERROR_VEHICLE", "ERROR_SIZE", "FormatError", "FormatError", "FormatError", "FormatError", ["FormatError"] * 20

    for v_end in range(1, len(tokens) - 4):
         for s_len in (1, 2, 3):
            if v_end + s_len + 4 > len(tokens):
                continue
            
            vehicle_candidate = ' '.join(tokens[:v_end])
            size_candidate = ' '.join(tokens[v_end : v_end + s_len])
            product_ids = tokens[v_end + s_len:]
            
            if len(product_ids) < 4:
                continue

            # Support up to 24 IDs
            hb_ids = product_ids[:4]
            skus = product_ids[4:24] if len(product_ids) > 4 else []

            if not all((t.isdigit() and len(t) in (7, 8)) or t == "-" for t in hb_ids):
                continue

            return vehicle_candidate, size_candidate.replace(" ", ""), hb_ids[0], hb_ids[1], hb_ids[2], hb_ids[3], skus

    logger.error(f"❌ Failed to parse output: {raw_text}")
    return "ERROR_VEHICLE", "ERROR_SIZE", "FormatError", "FormatError", "FormatError", "FormatError", ["FormatError"] * 20

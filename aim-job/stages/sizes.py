import re
import pandas as pd

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

def parse_vehicle_split(vehicle_str: str, known_makes: set):
    """
    Splits 'VAUXHALL GRANDLAND X' -> ('VAUXHALL', 'GRANDLAND X')
    using the KNOWN_MAKES set.
    """
    v = str(vehicle_str or "").strip()
    upper_v = v.upper()
    
    # longest makes first to avoid partial matches
    sorted_makes = sorted(list(known_makes), key=len, reverse=True)
    
    best_make = "Unknown"
    best_model = v

    for make in sorted_makes:
        if upper_v.startswith(make):
            best_make = make
            remainder = v[len(make):].strip()
            best_model = remainder
            break
            
    def to_title(s):
        return " ".join([word.capitalize() for word in s.split()])

    return to_title(best_make), to_title(best_model)

def parse_size_split(size_str: str):
    """
    Splits '225/55 R18' or '225/55R18' -> ('225', '55', '18')
    """
    match = re.search(r'(\d{2,3})[/\\](\d{2,3}(?:\.\d+)?)\s*[A-Z]*\s*(\d{2})', str(size_str).upper())
    if match:
        return match.group(1), match.group(2), match.group(3)
    return None, None, None

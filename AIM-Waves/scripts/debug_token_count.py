
import os
import sys
from aim_waves.core.engine import fetch_feedback_from_bigquery, construct_prompt
from aim_waves.core.utils import normalize_string_for_comparison

# Mock Config if needed or rely on existing
# We need to setup the path to find aim_waves if running as script
sys.path.append(os.getcwd())

def count_tokens(text):
    return len(text) // 4  # Rough estimate

def run_debug():
    vehicle = "Volkswagen Golf"
    size = "205/55 R16"
    
    print(f"Fetching data for {vehicle} {size}...")
    feedback_data = fetch_feedback_from_bigquery(size, vehicle)
    print(f"Fetched {len(feedback_data)} rows.")
    
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
        clean_row = [c.replace("|", "/") for c in row]
        rows.append("|".join(clean_row))

    tyre_data_str = "\n".join(rows)
    print(f"Data String Chars: {len(tyre_data_str)}")
    
    # Construct Prompt
    text_input = construct_prompt(
        vehicle, size, tyre_data_str, 
        "", "anymodel", "", 
        None, "",
        15, 1.1, 0.9
    )
    
    print(f"Total Prompt Chars: {len(text_input)}")
    print(f"Estimated Tokens: {count_tokens(text_input)}")
    
    # Check duplicate inclusions
    if text_input.count(tyre_data_str) > 1:
        print("⚠️ WARNING: Data string appears multiple times in prompt!")

if __name__ == "__main__":
    run_debug()

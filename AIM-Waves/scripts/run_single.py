import argparse
import sys
import os
import json
import logging

# Ensure we can import aim_waves
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from aim_waves.core.engine import generate_recommendation
from aim_waves.core.utils import parse_recommendation_output

# Configure logging to show info
logging.basicConfig(level=logging.INFO)

def main():
    parser = argparse.ArgumentParser(description="Run AIM-Waves recommendation for a single Vehicle + Size")
    parser.add_argument("--vehicle", required=True, help="Vehicle name (e.g. 'VW GOLF')")
    parser.add_argument("--size", required=True, help="Tyre size (e.g. '205/55 R16')")
    parser.add_argument("--brand", help="Brand enhancer")
    parser.add_argument("--model", help="Override Model Name (e.g., 'gemini-2.5-flash-lite')") # Re-purposed/Clarified
    parser.add_argument("--season", help="Seasonal performance")
    parser.add_argument("--goldilocks", type=int, default=15, help="Goldilocks Zone %")
    parser.add_argument("--disable-search", action="store_true", help="Disable Vertex AI Search tools")
    
    args = parser.parse_args()

    print(f"🚗 Running Single CAM Analysis")
    print(f"   Vehicle: {args.vehicle}")
    print(f"   Size:    {args.size}")
    if args.model:
        print(f"   Model:   {args.model}")
    if args.disable_search:
        print(f"   Search:  DISABLED")
    print("-" * 60)

    try:
        raw_result = generate_recommendation(
            vehicle=args.vehicle,
            size=args.size,
            brand_enhancer=args.brand,
            # We map --model CLI arg to override_model in engine
            override_model=args.model, 
            seasonal_performance=args.season,
            goldilocks_zone_pct=args.goldilocks,
            disable_search=args.disable_search
        )
        
        print("\n📄 Raw Engine Output:")
        print(f"'{raw_result}'\n")

        print("-" * 60)
        print("🧩 Parsed Result:")
        
        veh_out, size_out, hb1, hb2, hb3, hb4, skus = parse_recommendation_output(raw_result)
        
        success = hb1.isdigit() and hb2.isdigit()
        
        result_obj = {
            "Vehicle": veh_out,
            "Size": size_out,
            "Hotboxes": [hb1, hb2, hb3, hb4],
            "SKUs": skus,
            "Success": success
        }
        
        print(json.dumps(result_obj, indent=2))
        
        if not success:
            print("\n❌ Parsing failed or result malformed.")
            sys.exit(1)
        else:
            print("\n✅ Valid recommendation generated.")

    except Exception as e:
        print(f"\n❌ Error executing engine: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

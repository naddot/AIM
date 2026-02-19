import subprocess
import re
import os
import time
import json

# Test Matrix
MODELS = [
    "gemini-2.5-flash",
    "gemini-2.5-flash-lite"
]

SCENARIOS = [
    {"name": "With Tools", "flag": []},
    {"name": "No Tools", "flag": ["--disable-search"]}
]

TEST_VEHICLE = "MINI HATCH"
TEST_SIZE = "205/45 R17"

# Pricing (Approximate per 1M tokens)
PRICING = {
    "gemini-2.5-flash-lite": {"input": 0.10, "output": 0.40},
    "gemini-2.5-flash":      {"input": 0.10, "output": 0.40}, 
}

def analyze_skus(ref_skus, result_skus):
    """
    Compare result_skus against ref_skus.
    Returns:
    - missing: set
    - added: set
    - out_of_order: list of tuples (sku, expected_index, actual_index) - simplified check
    - is_sorted_relative: bool
    """
    ref_set = set(ref_skus)
    res_set = set(result_skus)
    
    missing = [s for s in ref_skus if s not in res_set]
    added = [s for s in result_skus if s not in ref_set]
    
    # Check Order of common items
    common_in_result = [s for s in result_skus if s in ref_set]
    
    # Map SKU -> Ref Index
    ref_idx_map = {sku: i for i, sku in enumerate(ref_skus)}
    
    is_sorted = True
    out_of_order_details = []
    
    last_idx = -1
    for i, sku in enumerate(common_in_result):
        curr_ref_idx = ref_idx_map[sku]
        if curr_ref_idx < last_idx:
            is_sorted = False
            out_of_order_details.append(sku)
        last_idx = curr_ref_idx
            
    return {
        "missing": missing,
        "added": added,
        "out_of_order": out_of_order_details,
        "is_sorted": is_sorted,
        "count_ref": len(ref_skus),
        "count_res": len(result_skus)
    }

def run_test(model, scenario_name, flags):
    print(f"\n🧪 Model: {model} | Mode: {scenario_name}")
    start_time = time.time()
    
    cmd = ["python", "scripts/run_single.py", 
           "--vehicle", TEST_VEHICLE, 
           "--size", TEST_SIZE, 
           "--model", model] + flags
           
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding='utf-8',
        errors='replace'
    )
    
    duration = time.time() - start_time
    output = result.stdout + result.stderr
    
    # Extract Metrics
    token_match = re.search(r"~\s*([\d,]+)\s*tokens", output)
    input_tokens = int(token_match.group(1).replace(",", "")) if token_match else 0
    
    # Extract Reference SKUs
    ref_match = re.search(r"DEBUG_REF_SKUS:\s*(\[.*?\])", output)
    ref_skus = json.loads(ref_match.group(1)) if ref_match else []
    
    # Extract Result SKUs
    parsed_match = re.search(r"🧩 Parsed Result:\s*(\{.*?\})", output, re.DOTALL)
    success = "✅ Valid recommendation generated" in output
    
    result_skus = []
    hotboxes = []
    
    if parsed_match:
        try:
            data = json.loads(parsed_match.group(1))
            hotboxes = data.get("Hotboxes", [])
            # Combine Hotboxes and List SKUs (ignoring dashes/placeholders)
            # The prompt logic typically puts top items in hotboxes
            # We treat the full ordered output as Hotboxes + SKUs
            raw_list = data.get("Hotboxes", []) + data.get("SKUs", [])
            result_skus = [s for s in raw_list if s and s != "-" and s.isdigit()]
        except:
            pass

    # Cost
    price = PRICING.get(model, {"input": 0.10, "output": 0.40})
    cost = (input_tokens / 1_000_000) * price["input"] + (100 / 1_000_000) * price["output"]
    
    analysis = analyze_skus(ref_skus, result_skus) if ref_skus and result_skus else None

    return {
        "model": model,
        "mode": scenario_name,
        "duration": duration,
        "tokens": input_tokens,
        "cost": cost,
        "success": success,
        "ref_skus": ref_skus,
        "result_skus": result_skus,
        "analysis": analysis
    }

def main():
    print("🚀 Starting Extended Matrix Benchmark...")
    print(f"   Target: {TEST_VEHICLE} {TEST_SIZE}")
    print("=" * 100)
    
    for model in MODELS:
        for scenario in SCENARIOS:
            res = run_test(model, scenario["name"], scenario["flag"])
            
            status_icon = "✅" if res["success"] else "❌"
            print(f"📊 {res['model']} ({res['mode']}) - {status_icon}")
            print(f"   ⏱️ Time: {res['duration']:.2f}s | 💰 Cost: ${res['cost']:.5f} | 🔢 Tokens: {res['tokens']:,}")
            
            if res["analysis"]:
                a = res["analysis"]
                print(f"   📦 SKUs: {a['count_res']} returned (Ref: {a['count_ref']})")
                print(f"   ⚖️ Sorted? {a['is_sorted']}")
                
                if a["out_of_order"]:
                     print(f"   ⚠️ Out of Order SKUs: {a['out_of_order']}")
                
                if a["added"]:
                    print(f"   👻 Hallucinated (Added) SKUs: {a['added']}")
                    
                # Print full comparison lists if requested (showing deviation)
                print(f"   📝 Full Output List: {res['result_skus']}")
                
                # Check for major deviations
                if not a["is_sorted"] or a["added"]:
                    print("   ❌ ALERT: Output violates strict ordering or contains hallucinations.")
            else:
                if not res["success"]:
                    print("   ❌ Failed to generate valid output.")
                elif not res["ref_skus"]:
                    print("   ⚠️ Reference SKUs not found in debug log.")
            
            print("-" * 100)

if __name__ == "__main__":
    main()

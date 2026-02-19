import argparse
import csv
import statistics
import time
import os
import datetime
import random
import concurrent.futures
from aim_waves.core.engine import generate_recommendation

# Constants
REPEATS = 3
WARMUP_DISCARD = 1
MAX_CONCURRENT = 20
MAX_RETRIES = 5
BASE_DELAY = 2      

PRICING = {
    "gemini-2.5-flash": {
        "input": 0.30,
        "output": 2.50
    },
    "gemini-2.5-flash-lite": {
        "input": 0.10,
        "output": 0.40
    }
}

VEHICLES = [
    ("Volkswagen Golf", "205/55 R16"),
    ("Ford Fiesta", "195/55 R16"),
    ("BMW 3 Series", "225/45 R17"),
    ("Audi A3", "225/40 R18"),
    ("Mercedes A-Class", "205/55 R16"),
    ("Nissan Qashqai", "215/55 R18"),
]

MODELS = ["gemini-2.5-flash", "gemini-2.5-flash-lite"]
SEARCH_SETTINGS = [True, False] 
THINKING_BUDGETS = [0, 2048] 

def calculate_cost(model_name, input_tokens, output_tokens):
    price_cfg = PRICING.get(model_name, {"input": 0.0, "output": 0.0})
    cost = (input_tokens / 1_000_000 * price_cfg["input"]) + \
           (output_tokens / 1_000_000 * price_cfg["output"])
    return cost

def execute_single_run(args):
    vehicle, size, model, search_enabled, thinking_budget, run_id = args
    retries = 0
    while retries <= MAX_RETRIES:
        try:
            if retries == 0:
                time.sleep(random.uniform(0.1, 1.0))
            result = generate_recommendation(
                vehicle=vehicle, size=size,
                override_model=model, disable_search=(not search_enabled),
                thinking_budget=thinking_budget, benchmark_mode=True,
                stream=False, return_metadata=True
            )
            if result.get("success") is False and result.get("error_type") in ["APIError", "StreamError"]:
                 raise Exception(f"Transient Error: {result.get('error_type')}")
            return result, None
        except Exception as e:
            retries += 1
            if retries > MAX_RETRIES:
                return {"success": False, "error_type": "MaxRetriesExceeded", "latency_ms": 0, "total_ms": 0, "usage": {}, "output": ""}, str(e)
            delay = (BASE_DELAY * (2 ** (retries - 1))) + random.uniform(0, 1)
            time.sleep(delay)

def run_benchmark(limit=None, repeats=REPEATS, max_concurrent=MAX_CONCURRENT, output_file=None):
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    report_filename = output_file if output_file else f"benchmark_report_{timestamp}.csv"
    
    fieldnames = [
        "Model", "Search_Enabled", "Thinking_Budget", "Vehicle", "Size",
        "Run_ID", "Success", "Model_Latency_ms", "E2E_Latency_ms",
        "Input_Tokens", "Output_Tokens", "Total_Tokens", "Cost_USD", "Error_Type",
        "Generated_SKUs", "Raw_Output"
    ]
    
    jobs = []
    count = 0
    limit_cutoff = limit * repeats if limit else float('inf')
    
    for vehicle, size in VEHICLES:
        for model in MODELS:
            for search_enabled in SEARCH_SETTINGS:
                for thinking_budget in THINKING_BUDGETS:
                    if count >= limit_cutoff: break
                    for i in range(repeats):
                        jobs.append((vehicle, size, model, search_enabled, thinking_budget, i))
                    count += repeats
        if count >= limit_cutoff: break

    print(f"🚀 Starting Benchmark (ROBUST PARSER ACTIVE) using {max_concurrent} threads.")
    print(f"📋 Total Jobs: {len(jobs)}")
    
    with open(report_filename, mode='w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrent) as executor:
            future_to_job = {executor.submit(execute_single_run, job): job for job in jobs}
            completed_count = 0
            for future in concurrent.futures.as_completed(future_to_job):
                vehicle, size, model, search_enabled, thinking_budget, run_id = future_to_job[future]
                completed_count += 1
                try:
                    result, error_msg = future.result()
                    usage = result.get("usage", {})
                    in_tokens = usage.get("prompt_token_count", 0)
                    out_tokens = usage.get("candidates_token_count", 0)
                    cost = calculate_cost(model, in_tokens, out_tokens)
                    
                    row = {
                        "Model": model, "Search_Enabled": search_enabled, "Thinking_Budget": thinking_budget,
                        "Vehicle": vehicle, "Size": size, "Run_ID": run_id,
                        "Success": result.get("success", False),
                        "Model_Latency_ms": result.get("latency_ms", 0),
                        "E2E_Latency_ms": result.get("total_ms", 0),
                        "Input_Tokens": in_tokens, "Output_Tokens": out_tokens,
                        "Total_Tokens": usage.get("total_token_count", 0),
                        "Cost_USD": f"{cost:.6f}",
                        "Error_Type": result.get("error_type", "") or (error_msg if error_msg else ""),
                        "Generated_SKUs": result.get("output", "") if result.get("success") else "",
                        "Raw_Output": result.get("output", "") # Raw output for failures, sanitized for success
                    }
                    writer.writerow(row)
                    csvfile.flush()
                    if completed_count % 10 == 0: print(f"   ✅ Progress: {completed_count}/{len(jobs)} jobs completed.")
                except Exception as exc:
                    print(f"   ❌ Job exception: {exc}")

    print(f"\n🏁 Benchmark Complete. Results saved to {report_filename}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark Gemini 2.5 Models with Robust Parser")
    parser.add_argument("--limit", type=int, help="Limit number of vehicle combinations to test")
    parser.add_argument("--repeats", type=int, default=REPEATS, help="Number of repeats per test")
    parser.add_argument("--concurrent", type=int, default=MAX_CONCURRENT, help="Max concurrent requests")
    parser.add_argument("--output_file", type=str, help="Custom output filename")
    args = parser.parse_args()
    run_benchmark(limit=args.limit, repeats=args.repeats, max_concurrent=args.concurrent, output_file=args.output_file)

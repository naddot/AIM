import subprocess
import re
import os
import time

TEST_CASES = [
    ("MINI HATCH", "205/45 R17"),
    ("BMW 3 SERIES", "225/45 R18"),
    ("FORD FIESTA", "195/55 R16")
]

# Gemini 2.5 Flash-Lite Pricing
INPUT_PRICE_PER_1M = 0.10
OUTPUT_PRICE_PER_1M = 0.40

def run_test(vehicle, size):
    print(f"\n🧪 Testing: {vehicle} | {size}")
    start_time = time.time()
    
    # Run the single script
    # We pass the API key via env var if running locally (it's already set in the terminal session hopefully, or we pass it here)
    # The agent tool 'run_command' sets it for the session, but here I am running a python script from a python script.
    # I will assume the parent env has it or I'll inject it if needed. 
    # For now, let's rely on the environment the user/agent is in.
    
    result = subprocess.run(
        ["python", "scripts/run_single.py", "--vehicle", vehicle, "--size", size],
        capture_output=True,
        text=True,
        encoding='utf-8',
        errors='replace' # Handle any encoding issues 
    )
    
    duration = time.time() - start_time
    output = result.stdout + result.stderr
    
    # Extract Token Count
    # Log format: 📝 Generated Prompt: 213,735 chars (~53,433 tokens)
    token_match = re.search(r"~\s*([\d,]+)\s*tokens", output)
    input_tokens = int(token_match.group(1).replace(",", "")) if token_match else 0
    
    # Extract Parsed Result
    # We look for the JSON block or the success line
    # Simple extraction for report
    parsed_match = re.search(r"🧩 Parsed Result:\s*(\{.*?\})", output, re.DOTALL)
    parsed_json = parsed_match.group(1) if parsed_match else "JSON Not Found"
    
    # Calculate Cost
    input_cost = (input_tokens / 1_000_000) * INPUT_PRICE_PER_1M
    output_cost = (100 / 1_000_000) * OUTPUT_PRICE_PER_1M # Est 100 output tokens
    total_cost = input_cost + output_cost
    
    print(f"   ⏱️ Duration: {duration:.2f}s")
    print(f"   🔢 Tokens: {input_tokens:,}")
    print(f"   💰 Est. Cost: ${total_cost:.5f}")
    
    # Print SKUs from the JSON if possible to show comparison
    import json
    try:
        data = json.loads(parsed_json)
        hbs = data.get("Hotboxes", [])
        print(f"   📦 Hotboxes: {hbs}")
    except:
        print(f"   ❌ Could not parse JSON output")

    print("-" * 60)

if __name__ == "__main__":
    print(f"🚀 Starting Cost & Quality Test")
    print(f"   Model: gemini-2.5-flash-lite")
    print(f"   Prices: Input ${INPUT_PRICE_PER_1M}/1M | Output ${OUTPUT_PRICE_PER_1M}/1M")
    print("-" * 60)
    
    for v, s in TEST_CASES:
        run_test(v, s)

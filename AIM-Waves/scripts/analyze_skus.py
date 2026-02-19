import pandas as pd
import collections
import sys

def analyze_report(file_path):
    df = pd.read_csv(file_path)
    
    # Filter for successful runs
    success_df = df[df['Success'] == True].copy()
    
    if success_df.empty:
        print("No successful runs found to analyze.")
        return

    # Clean the SKUs: The column format is "VEHICLE SIZE SKU1 SKU2 ..." 
    # We need to extract just the SKUs.
    def extract_skus(row):
        parts = row['Generated_SKUs'].split()
        # Find index where skus start (after vehicle and size)
        # Size is usually 1-2 tokens. Let's look for numbers starting after those.
        # More reliably: product IDs are digits.
        return [p for p in parts if p.isdigit() or p == '-']

    success_df['sku_list'] = success_df.apply(extract_skus, axis=1)
    
    # Group by config to check consistency
    groups = success_df.groupby(['Model', 'Search_Enabled', 'Thinking_Budget', 'Vehicle', 'Size'])
    
    print(f"--- SKU PATTERN ANALYSIS ({file_path}) ---")
    print(f"Analyzed {len(success_df)} successful runs.\n")
    
    results = []
    
    for name, group in groups:
        model, search, thinking, vehicle, size = name
        
        # 1. Consistency Check
        # Compare strings of SKU lists
        sku_strings = group['sku_list'].apply(lambda x: " ".join(x)).tolist()
        unique_patterns = set(sku_strings)
        consistency_pct = (sku_strings.count(max(sku_strings, key=sku_strings.count)) / len(sku_strings)) * 100
        
        # 2. Frequent SKUs in this group
        all_skus = [sku for sublist in group['sku_list'] for sku in sublist if sku != '-']
        counts = collections.Counter(all_skus)
        top_3 = [f"{sku}({count})" for sku, count in counts.most_common(3)]
        
        results.append({
            "Model": model,
            "Search": search,
            "Thinking": thinking,
            "Vehicle": vehicle,
            "Consistency": f"{consistency_pct:.0f}%",
            "Top SKUs": ", ".join(top_3)
        })

    results_df = pd.DataFrame(results)
    print(results_df.to_markdown(index=False))
    
    print("\n--- GLOBAL TOP SKUs PER VEHICLE ---")
    vehicle_groups = success_df.groupby(['Vehicle', 'Size'])
    for name, group in vehicle_groups:
        v, s = name
        all_skus = [sku for sublist in group['sku_list'] for sku in sublist if sku != '-']
        counts = collections.Counter(all_skus)
        print(f"{v} {s}: {', '.join([f'{sku}({c})' for sku, c in counts.most_common(5)])}")

if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "benchmark_report_20260204_182716.csv"
    analyze_report(path)

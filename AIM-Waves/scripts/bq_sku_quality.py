import pandas as pd
from google.cloud import bigquery
import sys
import os

def analyze_quality(csv_path):
    # 1. Load the benchmark results
    df = pd.read_csv(csv_path)
    
    # 2. Extract unique SKUs
    all_skus = set()
    for _, row in df.iterrows():
        if row['Success'] == True:
            skus = [s for s in row['Generated_SKUs'].split() if s.isdigit()]
            all_skus.update(skus)
    
    if not all_skus:
        print("No SKUs found to analyze.")
        return

    print(f"Checking metadata for {len(all_skus)} unique SKUs in BigQuery...")

    # 3. Query BigQuery for SKU metadata
    client = bigquery.Client()
    
    # Format the list for the SQL IN clause
    sku_list_str = ",".join([f"'{s}'" for s in all_skus])
    
    query = f"""
    SELECT 
        CAST(ProductId AS STRING) as ProductId,
        TyreScore,
        GRADE,
        BRAND,
        Model,
        PRICE,
        SalesStatus,
        Units
    FROM `bqsqltesting.nexus_tyrescore.TyreScore_algorithm_output`
    WHERE CAST(ProductId AS STRING) IN ({sku_list_str})
    """
    
    metadata_df = client.query(query).to_dataframe(create_bqstorage_client=False)
    
    # Deduplicate metadata_df: ProductId might repeat across vehicles
    # Sort by Units descending and drop duplicates to get the most "Active" version
    metadata_df = metadata_df.sort_values('Units', ascending=False).drop_duplicates('ProductId')
    
    # 4. Integrate back into our results
    # We want to see: For each run, what was the average TyreScore? What % were "BEST"?
    
    # Create a lookup dict
    meta_map = metadata_df.set_index('ProductId').to_dict('index')
    
    def grade_skus(sku_str):
        if not sku_str or pd.isna(sku_str): return []
        skus = [s for s in sku_str.split() if s.isdigit()]
        return [meta_map.get(s, {}).get('TyreScore', 'Unknown') for s in skus]

    def count_best(grades):
        return sum(1 for g in grades if 'BEST' in str(g).upper())

    df['SKU_Grades'] = df['Generated_SKUs'].apply(grade_skus)
    df['Best_Count'] = df['SKU_Grades'].apply(count_best)
    df['Avg_Best_Pct'] = (df['Best_Count'] / 20) * 100 # Assuming 20 targets

    # 5. Report results grouped by Model/Config/Thinking
    summary = df[df['Success']==True].groupby(['Model', 'Search_Enabled', 'Thinking_Budget']).agg({
        'Best_Count': 'mean',
        'Avg_Best_Pct': 'mean',
        'Run_ID': 'count'
    }).reset_index()

    print("\n--- SKU QUALITY ANALYSIS BY REASONING/SEARCH ---")
    print(summary.to_markdown(index=False))

    # 6. SKU Richness Analysis: How many unique products does each config find?
    richness = []
    for (model, search, thinking), group in df[df['Success']==True].groupby(['Model', 'Search_Enabled', 'Thinking_Budget']):
        unique_in_group = set()
        for _, row in group.iterrows():
            unique_in_group.update([s for s in row['Generated_SKUs'].split() if s.isdigit()])
        richness.append({
            "Model": model, "Search": search, "Thinking": thinking, "Unique_SKUs": len(unique_in_group)
        })
    
    print("\n--- SKU RICHNESS (Product Discovery) ---")
    print(pd.DataFrame(richness).to_markdown(index=False))

    # 7. Top Unmatched SKUs...
    found_skus = set(metadata_df['ProductId'].tolist())
    missing = all_skus - found_skus
    if missing:
        print(f"\n⚠️ {len(missing)} SKUs picked by model were NOT found in TyreScore_algorithm_output table.")
        print(f"Examples: {list(missing)[:10]}")

if __name__ == "__main__":
    csv_file = sys.argv[1] if len(sys.argv) > 1 else "benchmark_report_20260204_182716.csv"
    analyze_quality(csv_file)

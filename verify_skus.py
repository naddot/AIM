import pandas as pd
import glob
import os

files = glob.glob('demo/output/results_*.csv')
if not files:
    print("No results files found.")
    exit(1)

latest_file = max(files, key=os.path.getmtime)
print(f"Verifying {latest_file}...")

df = pd.read_csv(latest_file)
SKU_COLS = ['HB1', 'HB2', 'HB3', 'HB4'] + [f'SKU{i}' for i in range(1, 13)] # The CSV header from previous logs showed SKU16, but let's check what we got.

# Check for '-' or NaN
for idx, row in df.iterrows():
    print(f"\nRow {idx}: {row['Vehicle']} {row['Size']}")
    row_skus = []
    for col in df.columns:
        if col in ['HB1', 'HB2', 'HB3', 'HB4'] or col.startswith('SKU'):
            val = str(row[col]).strip()
            row_skus.append(val)
            if val == '-' or not val or val == 'nan':
                print(f"  ❌ Missing SKU in {col}")
    
    # Check for duplicates in row
    clean_skus = [s for s in row_skus if s.isdigit()]
    if len(clean_skus) != len(set(clean_skus)):
        print(f"  ❌ DUPLICATES FOUND: {clean_skus}")
    else:
        print(f"  ✅ All {len(clean_skus)} SKUs are unique.")
    
    if len(clean_skus) < 20 and len(df.columns) >= 22: # 2 (Veh/Size) + 4 (HB) + 16 (SKU)
         print(f"  ⚠️ Only {len(clean_skus)} unique SKUs found.")

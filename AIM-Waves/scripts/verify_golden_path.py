import sys
import os
import requests
import logging

# Ensure we can import aim_waves
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from aim_waves.core.contracts import RecommendationResult

# Setup
BASE_URL = "http://localhost:5000"
LOGIN_URL = f"{BASE_URL}/login"
API_URL = f"{BASE_URL}/api/recommendations"
APP_PASSWORD = os.environ.get("APP_ACCESS_PASSWORD", "dev-password")

# Configure check (Exit code 1 on failure)
def check(condition, message):
    if condition:
        print(f"✅ PASS: {message}")
    else:
        print(f"❌ FAIL: {message}")
        sys.exit(1)

def verify_golden_path():
    print("🌟 Starting Golden Path Verification")
    session = requests.Session()
    
    # 1. Login
    resp = session.post(LOGIN_URL, data={"password": APP_PASSWORD})
    check(resp.status_code == 200, "Authentication successful")

    # 2. Request Data (Batch)
    # We trigger a batch run. For Golden Path, we check if AT LEAST ONE valid result is returned.
    params = {"top_n": 5, "offset": 0}
    resp = session.get(API_URL, params=params)
    check(resp.status_code == 200, "API returned 200 OK")
    
    data = resp.json()
    check(isinstance(data, list) and len(data) > 0, "API returned a non-empty list")

    # 3. Contract Verification
    print(f"🔍 Validating {len(data)} items against contract...")
    for i, item in enumerate(data):
        try:
            # Pydantic validation
            rec = RecommendationResult(**item)
            
            # Business Rule: HB1 should not be Budget? (Hard to verify without knowing SKU grades here)
            # But we can verify structure is perfect.
            check(len(rec.SKUs) == 16, f"Item {i} has 16 SKUs")
            check(rec.success is True, f"Item {i} marked as success")
            
        except Exception as e:
            print(f"❌ Item {i} failed contract validation: {e}")
            sys.exit(1)
            
    print("🏆 Golden Path Verification Complete!")

if __name__ == "__main__":
    verify_golden_path()

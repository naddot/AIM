import requests
import json
import os
from datetime import datetime

# Configuration
BASE_URL = "http://localhost:5000"
LOGIN_URL = f"{BASE_URL}/login"
API_URL = f"{BASE_URL}/api/recommendations"
APP_PASSWORD = os.environ.get("APP_ACCESS_PASSWORD", "dev-password")

def test_login(session):
    print(f"🔑 Authenticating with password: {APP_PASSWORD}...")
    response = session.post(LOGIN_URL, data={"password": APP_PASSWORD})
    if response.status_code == 200:
        print("✅ Login successful")
        return True
    else:
        print(f"❌ Login failed: {response.status_code} - {response.text}")
        return False

def test_recommendation(session, vehicle, size):
    print(f"\n🚙 Testing recommendation for {vehicle} ({size})...")
    params = {
        "top_n": 5,
        "offset": 0,
        "goldilocks_zone_pct": 20
    }
    # Note: The API technically gets vehicle/size from the logic inside engine based on batch/csv? 
    # Wait, the current API `generate_batch_recommendations` iterates over the CSV data loaded in memory.
    # The API parameters filter what is returned, but the request doesn't specify a SINGLE vehicle/size to process 
    # unless we added filtering params to `generate_batch_recommendations`.
    # Let's check `routes.py` and `engine.py`.
    # `generate_batch_recommendations` takes `pod_filter` and `segment_filter`. 
    # It does NOT take a specific vehicle/size to run just one, unless we filter the in-memory map.
    # The current logic iterates `top_entries`.
    
    # So we can't easily test just "one" specific vehicle unless we use the pod/segment filters or rely on it being in the top N.
    
    response = session.get(API_URL, params=params)
    
    if response.status_code == 200:
        data = response.json()
        print(f"✅ Status 200 OK. Received {len(data)} results.")
        if data:
            print("   Sample Result:", json.dumps(data[0], indent=2))
        else:
            print("   ⚠️ No results returned (might be empty CSV or filters).")
    else:
        print(f"❌ Request failed: {response.status_code} - {response.text}")

def main():
    print("🚀 Starting Local Integration Test")
    print(f"Target: {BASE_URL}")
    
    session = requests.Session()
    
    # 1. Health Check
    try:
        health = session.get(f"{BASE_URL}/health")
        if health.status_code == 200:
            print("✅ /health check passed")
        else:
            print(f"❌ /health check failed: {health.status_code}")
            return
    except requests.exceptions.ConnectionError:
        print("❌ Could not connect to localhost:5000. Is the server running?")
        print("   Run: python run.py")
        return

    # 2. Login
    if not test_login(session):
        return

    # 3. Test API
    # Since we rely on the CSV loaded in the app, we just trigger the batch generation.
    test_recommendation(session, "ANY", "ANY")

if __name__ == "__main__":
    main()

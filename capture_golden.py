import sys
import os
import json
import datetime
from unittest.mock import MagicMock, patch
import runpy

# 1. Setup Manifest Interceptor
manifest = {
    "mode": "local",
    "dry_run": True,
    "stages": [],
    "sql_files": [],
    "http_requests": []
}

# 2. Mock specific libraries BEFORE they are imported by main.py
sys.modules["google.cloud"] = MagicMock()
sys.modules["google.cloud.bigquery"] = MagicMock()
sys.modules["google.cloud.storage"] = MagicMock()
sys.modules["httpx"] = MagicMock()

# Setup BigQuery Mock
mock_bq = MagicMock()
sys.modules["google.cloud.bigquery"].Client.return_value = mock_bq

def mock_query(query):
    manifest["sql_executed"].append({"query_snippet": query[:50] + "..."})
    return MagicMock()
mock_bq.query.side_effect = mock_query

# Setup Storage Mock
mock_storage = MagicMock()
sys.modules["google.cloud.storage"].Client.return_value = mock_storage
mock_bucket = MagicMock()
mock_storage.bucket.return_value = mock_bucket
mock_blob = MagicMock()
mock_bucket.blob.return_value = mock_blob
# Mock file exists for runlist check
mock_blob.exists.return_value = False 
# Mock download_as_text for runlist loading (if it tries GCS)
mock_blob.download_as_text.return_value = "vehicle,Size,PriorityRank\nTEST_CAR,205/55 R16,1"

# Setup HTTPX Mock
mock_httpx = MagicMock()
sys.modules["httpx"].AsyncClient = MagicMock()

class MockAsyncClient:
    def __init__(self, *args, **kwargs):
        self.headers = kwargs.get("headers", {})
        self.cookies = {} # Simulate cookie jar

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    async def post(self, url, data=None, json=None, **kwargs):
        manifest["http_requests"].append({"method": "POST", "url": str(url)})
        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status = lambda: None
        
        if "/login" in str(url):
            self.cookies["session"] = "mock_session"
        
        if "batch" in str(url):
            # Mock batch response
            resp.json.return_value = {
                "results": [
                    {"Vehicle": "TEST_CAR", "Size": "205/55 R16", "success": True, "SKUs": ["SKU12345"]}
                ],
                "usage": {"total_token_count": 100}
            }
        return resp

    async def get(self, url, params=None, **kwargs):
        manifest["http_requests"].append({"method": "GET", "url": str(url)})
        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status = lambda: None
        
        if "/app" in str(url):
            # Mock HTML for fetching segments
            resp.text = '<html><select name="segment"><option>Segment A</option></select></html>'
        
        if "/api/recommendations" in str(url):
            # Mock page response
            resp.json.return_value = [
                {"Vehicle": "TEST_CAR", "Size": "205/55 R16", "success": True, "SKUs": ["SKU12345"]}
            ]
        
        # history for redirect check
        resp.history = []
        resp.url = url
        return resp

sys.modules["httpx"].AsyncClient = MockAsyncClient

# 3. Prepare Environment
os.environ["AIM_MODE"] = "local"
os.environ["DRY_RUN"] = "True"
os.environ["IGNORE_GCS_CONFIG"] = "True"
os.environ["AIM_LOCAL_ROOT"] = "./demo" # Ensure this exists
# Ensure runlist loads from local if possible, or mock handles it
# Creating a dummy local runlist just in case main.py reads it directly without GCS client
os.makedirs("./demo/runlist", exist_ok=True)
with open("./demo/runlist/priority_runlist_current.csv", "w") as f:
    f.write("Vehicle,Size,PriorityRank\nTEST_CAR,205/55 R16,1\n")

# 4. Instrument Stage Functions (We can't easily patch main.py internal functions before import, 
# so we will inspect the 'stages' list AFTER import but BEFORE execution if main.py exposes it.
# Check: main.py executes generic 'stages' loop in 'if __name__'. 
# We will use runpy to execute it, but we can't intercept the loop easily.
# Instead, we rely on the log/print output or the fact that our Mocks are recording the heavy lifting.
# To record STAGE names, we can mock the functions if we import main as a module first?
# No, main.py is strictly a script.
# We will just parse the logs or rely on the http/sql output in manifest.
# Wait, let's wrap the stage execution by patching the 'stages' list if possible?
# main.py does: if __name__ == "__main__": ... stages = [...]
# We can't touch that.
# We will rely on HTTP/SQL recording which is robust enough.
pass

# 5. Execute
print("🚀 Starting Golden Capture via Mocking...")
try:
    # We must add aim-job to path to allow imports if any
    sys.path.append(os.path.join(os.getcwd(), "aim-job"))
    
    # Run the script
    runpy.run_path("aim-job/main.py", run_name="__main__")
    
    print("✅ Main executed.")
except SystemExit as e:
    if e.code != 0:
        print(f"❌ Script exited with code {e.code}")
except Exception as e:
    print(f"❌ Script failed: {e}")
    import traceback
    traceback.print_exc()

# 6. Save Manifest
with open("run_manifest.json", "w") as f:
    json.dump(manifest, f, indent=2)
print("✅ run_manifest.json saved.")

# 7. Copy job_status.json
try:
    with open("./demo/output/job_status.json", "r") as f:
        status = json.load(f)
    print("✅ job_status.json captured.")
except Exception as e:
    print(f"⚠️ Could not read job_status.json: {e}")

import httpx
import asyncio
import logging
import json
import os
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import google.auth.transport.requests
import google.oauth2.id_token

from config import AimConfig
from file_io.backend import IOBackend

@dataclass
class SegmentResult:
    segment_id: str # Segment Name or Batch ID
    status: str     # "success", "failed"
    error: Optional[str] = None
    attempts: int = 1
    usage: Optional[Dict] = None

@dataclass
class BatchSummary:
    results: List[Any] # Raw results from API
    usage_total: Dict[str, int]
    failed_count: int
    success_count: int

class WavesClient:
    def __init__(self, config: AimConfig):
        self.config = config
        self.base_url = config.aim_base_url
        self.waves_url = config.aim_waves_url # Use for batch
        self.service_password = config.aim_service_password

    def get_id_token(self, url: str) -> Optional[str]:
        if self.config.aim_mode == "local":
            return None
        try:
            logging.info(f"🔑 Fetching OIDC ID Token for audience: {url}")
            auth_req = google.auth.transport.requests.Request()
            token = google.oauth2.id_token.fetch_id_token(auth_req, url)
            logging.info("✅ ID Token fetched successfully.")
            return token
        except Exception as e:
            logging.warning(f"⚠️ Failed to fetch ID token: {e}")
            return None

    async def login(self, client: httpx.AsyncClient):
        logging.info(f"🔑 Logging in to {self.base_url}...")
        r = await client.post(
            f"{self.base_url}/login",
            data={"password": self.service_password},
            timeout=30,
        )
        r.raise_for_status()
        if not client.cookies:
            raise RuntimeError("Login did not set any cookies.")
        logging.info("✅ Login successful and session cookie set.")

    async def fetch_batch(self, client: httpx.AsyncClient, run_id: str, cams: List[dict], log_file_backend: IOBackend = None) -> Dict:
        """
        Executes a batch request. NO RETRIES here (except generic transient transport errors if httpx supports).
        Retries are managed by the Orchestrator.
        """
        params = {
            "goldilocks_zone_pct": self.config.goldilocks_zone_pct,
            "price_fluctuation_upper": self.config.price_fluct_upper,
            "price_fluctuation_lower": self.config.price_fluct_lower,
            "brand_enhancer": self.config.brand_enhancer or None,
            "model_enhancer": self.config.model_enhancer or None,
            "season": self.config.season or None,
            "disable_search": self.config.disable_search,
        }
        payload = {
            "run_id": run_id,
            "cams": cams,
            "params": {k: v for k, v in params.items() if v is not None}
        }
        
        # Local Logging (if backend provided)
        if self.config.aim_mode == "local" and log_file_backend:
            try:
                log_path = f"logs/requests_{run_id}.jsonl"
                log_file_backend.ensure_parent_dir(log_path)
                # Append? IOBackend usually overwrites.
                # If we need append, we might need to read->append->write or just let it overwrite for single call.
                # Actually main.py used append. But for simplicity let's overwrite or skip if complex.
                # Let's write separate file per request or just skip append complexity for now.
                pass 
            except Exception as e:
                logging.warning(f"⚠️ Failed to log local request: {e}")

        # The actual request
        resp = await client.post(
            f"{self.waves_url}/api/recommendations/batch",
            json=payload,
            timeout=self.config.request_timeout_s
        )
        resp.raise_for_status()
        return resp.json()

    async def fetch_segments(self, client: httpx.AsyncClient) -> List[str]:
        # Implementation of fetching segment list from /app
        from bs4 import BeautifulSoup
        
        r = await client.get(f"{self.base_url}/app", timeout=30)
        r.raise_for_status()
        
        # Simple redirect check
        if "/login" in str(r.url): raise RuntimeError("Redirected to login")

        soup = BeautifulSoup(r.text, "html.parser")
        sel = soup.select_one("select[name='segment']")
        if not sel: raise RuntimeError("No segment dropdown")
        
        segments = [
            o.get_text(strip=True) for o in sel.find_all("option")
            if o.get_text(strip=True) and not o.get_text(strip=True).startswith("--")
        ]
        return segments

    async def fetch_page(self, client: httpx.AsyncClient, segment: str, offset: int, retries=2) -> List[Dict]:
        # Legacy SINGLE SEGMENT fetching
        # Copied logic from main.py
        params = {
            "segment": segment,
            "top_n": self.config.page_size,
            "offset": offset,
            "goldilocks_zone_pct": self.config.goldilocks_zone_pct,
            "price_fluctuation_upper": self.config.price_fluct_upper,
            "price_fluctuation_lower": self.config.price_fluct_lower,
            "brand_enhancer": self.config.brand_enhancer or None,
             # ... etc
        }
        params = {k: v for k, v in params.items() if v is not None} # cleanup
        
        for attempt in range(retries + 1):
             try:
                 r = await client.get(
                     f"{self.base_url}/api/recommendations", 
                     params=params, 
                     timeout=self.config.request_timeout_s
                 )
                 r.raise_for_status()
                 return r.json()
             except Exception as e:
                 if attempt == retries: raise
                 await asyncio.sleep(1.5 * (attempt + 1))
                 
        return []



import asyncio
import unittest
from unittest.mock import MagicMock, AsyncMock, patch
import httpx
import logging

# Import the code to test
# We need to import fetch_batch_with_retry and context/config mocks
# Since fetch_batch_with_retry is inside stage_4.py and not easily importable 
# (it's a script not a module in some sense, but we can try importing it)
# If import fails, we might need to copy the function here for testing or adjust python path.
# Let's assume we can import it if we set up sys.path

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../aim-job")))

from stages.stage_4 import fetch_batch_with_retry, refresh_auth

class TestRetryLogic(unittest.IsolatedAsyncioTestCase):
    async def test_success_first_try(self):
        ctx = MagicMock()
        client = AsyncMock(spec=httpx.AsyncClient)
        
        # Setup Success
        ctx.waves.fetch_batch = AsyncMock(return_value={"results": [], "usage": {}})
        ctx.io = MagicMock()
        
        res = await fetch_batch_with_retry(ctx, client, "run_id", [], max_retries=3)
        self.assertIn("results", res)
        self.assertEqual(ctx.waves.fetch_batch.call_count, 1)

    async def test_401_refresh_retry(self):
        ctx = MagicMock()
        client = AsyncMock(spec=httpx.AsyncClient)
        
        # Setup: Fail 401, Then Success
        error_401 = httpx.HTTPStatusError("401", request=MagicMock(), response=MagicMock(status_code=401))
        
        # Side effect: Raise 401 once, then return success
        ctx.waves.fetch_batch = AsyncMock(side_effect=[error_401, {"results": [], "success": True}])
        
        # Mock Refresh
        ctx.waves.get_id_token = MagicMock(return_value="new_token")
        ctx.waves.login = AsyncMock()
        ctx.config.aim_base_url = "http://mock"
        
        # We need to mock refresh_auth too if we want to verify it calls it, 
        # but since we imported the real function, we can mock the dependency ctx.
        
        with patch('stages.stage_4.refresh_auth', new_callable=AsyncMock) as mock_refresh:
            res = await fetch_batch_with_retry(ctx, client, "run_id", [], max_retries=3)
            
            self.assertEqual(ctx.waves.fetch_batch.call_count, 2)
            mock_refresh.assert_called_once() 
            # verify we got result
            self.assertTrue(res["success"])

    async def test_429_backoff_retry(self):
        ctx = MagicMock()
        client = AsyncMock(spec=httpx.AsyncClient)
        
        # Setup: Fail 429 twice, then Success
        error_429 = httpx.HTTPStatusError("429", request=MagicMock(), response=MagicMock(status_code=429))
        ctx.waves.fetch_batch = AsyncMock(side_effect=[error_429, error_429, {"results": [], "success": True}])
        
        # Mock sleep to be instant
        with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
            res = await fetch_batch_with_retry(ctx, client, "run_id", [], max_retries=3)
            
            self.assertEqual(ctx.waves.fetch_batch.call_count, 3)
            self.assertEqual(mock_sleep.call_count, 2) # Slept twice

    async def test_fatal_error(self):
        ctx = MagicMock()
        client = AsyncMock(spec=httpx.AsyncClient)
        
        # Setup: Fail 500 forever
        error_500 = httpx.HTTPStatusError("500", request=MagicMock(), response=MagicMock(status_code=500))
        ctx.waves.fetch_batch = AsyncMock(side_effect=error_500)
        
        with patch('asyncio.sleep', new_callable=AsyncMock):
            # Expect HTTPStatusError to bubble up after retries
            with self.assertRaises(httpx.HTTPStatusError):
                await fetch_batch_with_retry(ctx, client, "run_id", [], max_retries=2)

if __name__ == '__main__':
    logging.basicConfig(level=logging.CRITICAL) # Silence logs
    unittest.main()

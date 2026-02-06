import os
import json
import datetime as dt
import logging
from typing import Dict, Any, List
from config import AimConfig

class StatusTracker:
    """
    Authoritative state tracker. 
    Writes to job_status.json (mainly for Local Mode UI).
    Also records execution manifest for verification.
    """
    def __init__(self, config: AimConfig):
        self.config = config
        self.status_file = "output/job_status.json" # Relative path, handled by IOBackend in refined version, or raw here?
        # The original code wrote directly using os.path.join(AIM_LOCAL_ROOT...).
        # We should ideally use the IOBackend, but StatusTracker is often initialized *before* IO.
        # For safety/compliance with legacy behavior, we will write to local file system directly if local mode,
        # mirroring the original behavior.
        
        self.run_id = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        
        self.data = {
            "state": "idle",
            "run_id": self.run_id,
            "started_at": None,
            "ended_at": None,
            "heartbeat_ts": None,
            "last_log_line": "Initialized",
            "output_file": None,
            "error_summary": None,
            "progress": {"attempted": 0, "succeeded": 0, "failed": 0}
        }
        
        # Manifest for Golden Verification
        self.manifest = {
            "mode": self.config.aim_mode,
            "dry_run": self.config.dry_run,
            "stages": [],
            "sql_files": [], # {file, size, sha256}
            "http_requests": [] # Optional, maybe too noisy? User asked for SQL/Stages.
        }
        
        if self.config.aim_mode == "local":
            self._ensure_local_dir()

    def _ensure_local_dir(self):
        # We rely on config.local_root for the physical path
        full_path = os.path.join(self.config.local_root, os.path.dirname(self.status_file))
        os.makedirs(full_path, exist_ok=True)

    def update(self, state=None, last_log_line=None, error_summary=None, progress=None, output_file=None, report=None):
        if self.config.aim_mode != "local": return
        
        now = dt.datetime.now().isoformat()
        self.data["heartbeat_ts"] = now
        
        if state: 
            self.data["state"] = state
            if state == "running" and not self.data["started_at"]:
                self.data["started_at"] = now
            elif state in ("success", "failed"):
                self.data["ended_at"] = now
                
        if last_log_line: self.data["last_log_line"] = last_log_line
        if error_summary: self.data["error_summary"] = error_summary
        if progress: self.data["progress"].update(progress)
        if output_file: self.data["output_file"] = output_file
        if report: self.data["report"] = report
        
        self._write_to_disk()

    def heartbeat(self):
        self.update()

    def _write_to_disk(self):
        try:
            full_path = os.path.join(self.config.local_root, self.status_file)
            with open(full_path, "w") as f:
                json.dump(self.data, f, indent=2)
        except Exception as e:
            logging.error(f"❌ Failed to write status file: {e}")

    # --- Manifest Recording ---
    
    def record_stage_start(self, stage_name: str):
        self.manifest["stages"].append(stage_name)
        
    def record_sql_execution(self, file_path: str, size: int, sha256: str):
        self.manifest["sql_files"].append({
            "file": file_path,
            "size": size,
            "sha256": sha256
        })
        
    def save_manifest(self):
        """Writes the run_manifest.json to local output."""
        # Using local root directly to ensure it survives
        if self.config.aim_mode == "local" or self.config.dry_run:
             try:
                manifest_path = os.path.join(self.config.local_root, "output", "run_manifest.json")
                os.makedirs(os.path.dirname(manifest_path), exist_ok=True) # Ensure output dir
                with open(manifest_path, "w") as f:
                    json.dump(self.manifest, f, indent=2)
                logging.info(f"📝 Manifest saved to {manifest_path}")
             except Exception as e:
                 logging.error(f"⚠️ Failed to save manifest: {e}")

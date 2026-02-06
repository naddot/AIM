import os
import glob
from typing import List
from .backend import IOBackend
import logging

class LocalBackend(IOBackend):
    def __init__(self, root_dir: str):
        self.root_dir = os.path.abspath(root_dir)
        if not os.path.exists(self.root_dir):
             # We allow creation of root if missing? Or strict?
             # Let's create it to be helpful for demo setup.
             logging.info(f"📂 Creating local root: {self.root_dir}")
             os.makedirs(self.root_dir, exist_ok=True)

    def resolve_path(self, path: str) -> str:
        # Sanitation
        if os.path.isabs(path):
            raise ValueError(f"Path must be relative: {path}")
        if ".." in path:
            raise ValueError(f"Path cannot contain '..': {path}")
        if path.startswith("gs://"):
            raise ValueError(f"LocalBackend cannot handle GCS URI: {path}")
            
        full_path = os.path.join(self.root_dir, path)
        return os.path.normpath(full_path)

    def read_text(self, path: str) -> str:
        full = self.resolve_path(path)
        with open(full, "r", encoding="utf-8") as f:
            return f.read()

    def read_bytes(self, path: str) -> bytes:
        full = self.resolve_path(path)
        with open(full, "rb") as f:
            return f.read()

    def write_text(self, path: str, content: str):
        full = self.resolve_path(path)
        self.ensure_parent_dir(path)
        with open(full, "w", encoding="utf-8") as f:
            f.write(content)

    def write_bytes(self, path: str, content: bytes):
        full = self.resolve_path(path)
        self.ensure_parent_dir(path)
        with open(full, "wb") as f:
            f.write(content)

    def exists(self, path: str) -> bool:
        full = self.resolve_path(path)
        return os.path.exists(full)

    def list_files(self, prefix: str, recursive: bool = True) -> List[str]:
        # Prefix acts like a folder path in local mode usually
        # But GCS prefix can be partial filename. 
        # To align, we treat 'prefix' as a directory path or partial path.
        # Let's support directory listing behavior primarily.
        
        full_prefix = self.resolve_path(prefix)
        results = []
        
        if os.path.isdir(full_prefix):
             # Walk directory
             for root, dirs, files in os.walk(full_prefix):
                 for name in files:
                     abs_path = os.path.join(root, name)
                     rel_path = os.path.relpath(abs_path, self.root_dir)
                     # Convert windows sep to standard to emulate bucket paths if needed?
                     # Standardize to forward slash for consistency with GCS
                     results.append(rel_path.replace("\\", "/"))
                 if not recursive:
                     break
        else:
            # File or partial match?
            # GCS prefix "foo/bar" matches "foo/bar_1.csv" and "foo/bar/baz.csv"
            # It's safer to rely on glob if we want that behavior.
            # For now, let's assume directory listing for simplicity in implementation plan usage
            pass

        return sorted(results)

    def ensure_parent_dir(self, path: str):
        full = self.resolve_path(path)
        parent = os.path.dirname(full)
        if not os.path.exists(parent):
            os.makedirs(parent, exist_ok=True)

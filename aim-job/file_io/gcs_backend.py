from typing import List
from google.cloud import storage
import logging
from .backend import IOBackend

class GCSBackend(IOBackend):
    def __init__(self, project_id: str, bucket_name: str, root_prefix: str = ""):
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.root_prefix = root_prefix.strip("/") # e.g. "aim-daily-files"
        try:
            self.client = storage.Client(project=project_id)
            self.bucket = self.client.bucket(bucket_name)
        except Exception as e:
            # Lazy init or failure? In cloud format, this should work.
            # We don't crash init for testing reasons unless needed.
            logging.warning(f"⚠️ GCS Client init warning (ignore if mocking): {e}")
            self.bucket = None

    def resolve_path(self, path: str) -> str:
        # e.g. "output/results.csv" -> "aim-daily-files/output/results.csv"
        if path.startswith("/"):
            raise ValueError(f"Path must be relative: {path}")
        if path.startswith("gs://"):
             raise ValueError(f"GCSBackend expects relative paths, not URI: {path}")
        
        # Join with root prefix
        if self.root_prefix:
            return f"{self.root_prefix}/{path}"
        return path

    def _get_blob(self, path: str):
        full_key = self.resolve_path(path)
        return self.bucket.blob(full_key)

    def read_text(self, path: str) -> str:
        blob = self._get_blob(path)
        return blob.download_as_text()

    def read_bytes(self, path: str) -> bytes:
        blob = self._get_blob(path)
        return blob.download_as_bytes()

    def write_text(self, path: str, content: str):
        blob = self._get_blob(path)
        blob.upload_from_string(content)

    def write_bytes(self, path: str, content: bytes):
        blob = self._get_blob(path)
        blob.upload_from_string(content)

    def exists(self, path: str) -> bool:
        blob = self._get_blob(path)
        return blob.exists()

    def list_files(self, prefix: str, recursive: bool = True) -> List[str]:
        full_prefix = self.resolve_path(prefix)
        # GCS list_blobs is recursive by default unless delimiter is used
        delimiter = None if recursive else "/"
        
        blobs = self.client.list_blobs(self.bucket, prefix=full_prefix, delimiter=delimiter)
        
        results = []
        for blob in blobs:
            # Return path relative to ROOT_PREFIX? Or just partial?
            # Contracts says: backend-relative paths.
            # If full key is "aim-daily-files/output/foo.csv" and root is "aim-daily-files"
            # we should return "output/foo.csv".
            
            name = blob.name
            if self.root_prefix and name.startswith(self.root_prefix):
                 # Strip root prefix + slash
                 rel = name[len(self.root_prefix):].lstrip("/")
                 results.append(rel)
            else:
                 results.append(name)
                 
        return sorted(results)

    def ensure_parent_dir(self, path: str):
        # No-op in GCS
        pass

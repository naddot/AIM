from abc import ABC, abstractmethod
from typing import List, Optional
import os

class IOBackend(ABC):
    """
    Abstract interface for File I/O.
    Enforces path sanitization and behavior consistency.
    """

    @abstractmethod
    def resolve_path(self, path: str) -> str:
        """
        Normalizes input path and joins with backend root.
        Must raise ValueError if path is absolute or invalid.
        """
        pass

    @abstractmethod
    def read_text(self, path: str) -> str:
        pass
    
    @abstractmethod
    def read_bytes(self, path: str) -> bytes:
        pass

    @abstractmethod
    def write_text(self, path: str, content: str):
        pass
    
    @abstractmethod
    def write_bytes(self, path: str, content: bytes):
        pass

    @abstractmethod
    def exists(self, path: str) -> bool:
        pass

    @abstractmethod
    def list_files(self, prefix: str, recursive: bool = True) -> List[str]:
        """
        Returns a SORTED list of backend-relative paths.
        """
        pass

    @abstractmethod
    def ensure_parent_dir(self, path: str):
        """
        Ensures the parent directory of 'path' exists.
        Relevant for Local; No-op for GCS.
        """
        pass

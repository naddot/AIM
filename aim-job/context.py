from dataclasses import dataclass
from typing import Any, Optional

from config import AimConfig

@dataclass
class Context:
    """Dependency Container for the AIM Job."""
    config: AimConfig
    tracker: Any  # StatusTracker
    io: Any       # IOBackend
    bq: Any       # BigQuery Client Wrapper
    waves: Any    # Waves Client
    
    # helper to facilitate typing later if needed, 
    # but for now we keep it loose to avoid circular imports during setup

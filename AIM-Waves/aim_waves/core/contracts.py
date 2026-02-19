from pydantic import BaseModel, Field, validator
from typing import List, Optional

class RecommendationInput(BaseModel):
    vehicle: str = Field(..., description="Vehicle name")
    size: str = Field(..., description="Tyre size")
    goldilocks_zone_pct: int = Field(15, ge=5, le=50)
    price_fluctuation_upper: float = Field(1.1, ge=1.0, le=2.0)
    price_fluctuation_lower: float = Field(0.9, ge=0.5, le=1.0)
    brand_enhancer: Optional[str] = None
    model_enhancer: Optional[str] = None
    pod_filter: Optional[str] = None
    segment_filter: Optional[str] = None
    seasonal_performance: Optional[str] = None

class RecommendationResult(BaseModel):
    Vehicle: str
    Size: str
    HB1: str
    HB2: str
    HB3: str
    HB4: str
    SKUs: List[str]
    success: bool

    @validator('SKUs')
    def validate_skus(cls, v):
        if len(v) != 20:
            raise ValueError(f"Must have exactly 20 SKUs, got {len(v)}")
        return v

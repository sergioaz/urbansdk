from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List, Dict, Any
from fastapi import Query
from app.helpers.periods import DayName, PeriodName, get_day_number, get_period_number

class SlowLinkRequest(BaseModel):
    """Request model for slow links"""
    
    period: PeriodName = Field(Query(..., description="Time period name"))
    threshold: float = Field(Query(..., description="Threshold for average speed"))
    min_days: int = Field(..., ge=1, le=7, description="Minimum number of days in a week (1-7)")

    def get_period_number(self) -> int:
        """Convert period name to period number"""
        return get_period_number(self.period)
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "period": "AM Peak",
                "threshold": 20.5,
                "min_days": 3
            }
        }
    )


class SlowLinkData(BaseModel):
    """Model for individual link data"""
    
    link_id: int = Field(..., description="Link identifier")
    geometry: Optional[str] = Field(None, description="WKT LINESTRING geometry")
    road_name: Optional[str] = Field(None, description="Name of the road")
    average_speed: float = Field(..., description="Average speed for this link")

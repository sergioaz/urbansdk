from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List, Dict, Any
from fastapi import Query
from app.helpers.periods import DayName, PeriodName, get_day_number, get_period_number

class SlowLinksRequest(BaseModel):
    """Request model for aggregate speed data"""
    
    period: PeriodName = Field(Query(..., description="Time period name"))
    threshold: float = Field(Query(..., description="Threshold speed for slow links"))
    min_days: int = Field(Query(..., description="Min days in a week for slow links"))

    
    def get_period_number(self) -> int:
        """Convert period name to period number"""
        return get_period_number(self.period)
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "period": "AM Peak",
                "threshold": 30.5,
                "min_days": 3
            }
        }
    )


class LinkData(BaseModel):
    """Model for individual link data"""
    
    link_id: int = Field(..., description="Link identifier")
    geometry: Optional[str] = Field(None, description="WKT LINESTRING geometry")
    road_name: Optional[str] = Field(None, description="Name of the road")
    overall_average_speed: float = Field(..., description="Calculated overall Average speed for this link")




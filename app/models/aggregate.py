from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List, Dict, Any
from fastapi import Query
from app.helpers.periods import DayName, PeriodName, get_day_number, get_period_number

class AggregateRequest(BaseModel):
    """Request model for aggregate speed data"""
    
    day: DayName = Field(Query(..., description="Day of the week name"))
    period: PeriodName = Field(Query(..., description="Time period name"))
    
    def get_day_number(self) -> int:
        """Convert day name to day number"""
        return get_day_number(self.day)
    
    def get_period_number(self) -> int:
        """Convert period name to period number"""
        return get_period_number(self.period)
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "day": "Tuesday",
                "period": "AM Peak"
            }
        }
    )


class LinkData(BaseModel):
    """Model for individual link data"""
    
    link_id: int = Field(..., description="Link identifier")
    geometry: Optional[str] = Field(None, description="WKT LINESTRING geometry")
    road_name: Optional[str] = Field(None, description="Name of the road")
    average_speed: float = Field(..., description="Average speed for this link")




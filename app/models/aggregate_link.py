from pydantic import BaseModel, Field, ConfigDict
from typing import Optional
from fastapi import Query
from app.helpers.periods import DayName, PeriodName, get_day_number, get_period_number


class AggregateLinkRequest(BaseModel):
    """Request model for link aggregate speed data"""
    
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


class AggregateLinkResponse(BaseModel):
    """Response model for link aggregate speed data with metadata"""
    link_id: int = Field(..., description="Unique identifier for the link")
    day_of_week: int = Field(..., description="Day of week (1-7, where 1=Monday)")
    period: int = Field(..., description="Time period identifier")
    average_speed: float = Field(..., description="Average speed for the link, day, and period")
    record_count: int = Field(..., description="Number of records contributing to the average")
    road_name: Optional[str] = Field(None, description="Name of the road")
    geometry: Optional[str] = Field(None, description="Geometry representation of the link")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "link_id": 1148855686,
                "day_of_week": 2,
                "period": 4,
                "average_speed": 42.75,
                "record_count": 10,
                "road_name": "Sample Road",
                "geometry": "LINESTRING(-81.51023 30.16599, -81.51038 30.16637)",
            }
        }
    )

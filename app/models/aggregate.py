from pydantic import BaseModel, Field, ConfigDict
from typing import Optional
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


class AggregateResponse(BaseModel):

    """Response model for aggregate speed data"""
    day_of_week: int = Field(..., description="Day of week (1-7, where 1=Monday)")
    period: int = Field(..., description="Time period identifier")
    average_speed: float = Field(..., description="Average speed for the day and period")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "day_of_week": 2,
                "period": 4,
                "average_speed": 42.75
            }
        }
    )


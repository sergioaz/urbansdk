from pydantic import BaseModel, Field
from typing import Optional

class AggregateResponse(BaseModel):

    """Response model for aggregate speed data"""
    day_of_week: int = Field(..., description="Day of week (1-7, where 1=Monday)")
    period: int = Field(..., description="Time period identifier")
    average_speed: float = Field(..., description="Average speed for the day and period")

    class Config:
        schema_extra = {
            "example": {
                "day_of_week": 2,
                "period": 4,
                "average_speed": 42.75
            }
        }


from pydantic import BaseModel, Field, field_validator, ConfigDict
from typing import List
from app.helpers.periods import DayName, PeriodName, get_day_number, get_period_number


class SpatialFilterIn(BaseModel):
    """Input model for spatial filtering with day, period, and bounding box"""
    
    day: DayName = Field(..., description="Day of the week name")
    period: PeriodName = Field(..., description="Time period name")
    bbox: List[float] = Field(..., description="Bounding box coordinates [west, south, east, north]")
    
    @field_validator('bbox')
    @classmethod
    def validate_bbox(cls, v):
        """Validate bounding box format and coordinates"""
        if len(v) != 4:
            raise ValueError("Bounding box must contain exactly 4 coordinates [west, south, east, north]")
        
        west, south, east, north = v
        
        # Validate longitude range (-180 to 180)
        if not (-180 <= west <= 180) or not (-180 <= east <= 180):
            raise ValueError("Longitude values (west, east) must be between -180 and 180")
        
        # Validate latitude range (-90 to 90)
        if not (-90 <= south <= 90) or not (-90 <= north <= 90):
            raise ValueError("Latitude values (south, north) must be between -90 and 90")
        
        # Validate logical order
        if west >= east:
            raise ValueError("West longitude must be less than east longitude")
        
        if south >= north:
            raise ValueError("South latitude must be less than north latitude")
        
        return v
    
    def get_day_number(self) -> int:
        """Convert day name to day number"""
        return get_day_number(self.day)
    
    def get_period_number(self) -> int:
        """Convert period name to period number"""
        return get_period_number(self.period)
    
    def get_bbox_tuple(self) -> tuple[float, float, float, float]:
        """Get bounding box as tuple (west, south, east, north)"""
        return tuple(self.bbox)

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "day": "Tuesday",
                "period": "AM Peak",
                "bbox": [-81.8, 30.1, -81.6, 30.3]
            }
        }
    )


class SpatialFilterResponse(BaseModel):
    """Response model for spatial filter containing link IDs"""

    link_ids: List[int] = Field(..., description="List of link IDs within the specified spatial and temporal filters")
    count: int = Field(..., description="Number of links found")
    day_of_week: int = Field(..., description="Day of week number (1-7, where 1=Sunday)")
    period: int = Field(..., description="Time period number (1-7)")
    bbox: List[float] = Field(..., description="Bounding box coordinates [west, south, east, north]")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "link_ids": [1148855686, 1240632857, 1240632858],
                "count": 3,
                "day_of_week": 3,
                "period": 3,
                "bbox": [-81.8, 30.1, -81.6, 30.3]
            }
        }
    )

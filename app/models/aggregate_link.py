from pydantic import BaseModel, Field
from typing import Optional


class AggregateLinkResponse(BaseModel):
    """Response model for link aggregate speed data with metadata"""
    link_id: int = Field(..., description="Unique identifier for the link")
    day_of_week: int = Field(..., description="Day of week (1-7, where 1=Monday)")
    period: int = Field(..., description="Time period identifier")
    average_speed: float = Field(..., description="Average speed for the link, day, and period")
    average_freeflow: float = Field(..., description="Average freeflow speed for the link, day, and period")
    record_count: int = Field(..., description="Number of records contributing to the average")
    length: float = Field(..., description="Length of the road segment")
    road_name: Optional[str] = Field(None, description="Name of the road")
    usdk_speed_category: Optional[int] = Field(None, description="USDK speed category")
    funclass_id: Optional[int] = Field(None, description="Functional class ID")
    speedcat: Optional[int] = Field(None, description="Speed category")
    volume_value: Optional[int] = Field(None, description="Volume value")
    volume_bin_id: Optional[int] = Field(None, description="Volume bin ID")
    volume_year: Optional[int] = Field(None, description="Volume year")
    volumes_bin_description: Optional[str] = Field(None, description="Volume bin description")
    geo_json: Optional[str] = Field(None, description="GeoJSON representation of the link")

    class Config:
        schema_extra = {
            "example": {
                "link_id": 1148855686,
                "day_of_week": 2,
                "period": 4,
                "average_speed": 42.75,
                "average_freeflow": 43.00,
                "record_count": 10,
                "length": 0.027340,
                "road_name": "Sample Road",
                "usdk_speed_category": 40,
                "funclass_id": 4,
                "speedcat": 2,
                "volume_value": 800,
                "volume_bin_id": 1,
                "volume_year": 2022,
                "volumes_bin_description": "0-1999",
                "geo_json": "{\"type\":\"MultiLineString\",\"coordinates\":[[[-81.51023,30.16599],[-81.51038,30.16637]]]}"
            }
        }

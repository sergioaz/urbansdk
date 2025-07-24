import logging
from fastapi import APIRouter, HTTPException
from app.models.spatial_filter import SpatialFilterIn, SpatialFilterResponse, LinkGeometry
from app.services.aggregate import get_links_with_geometries_in_box_day_period

router = APIRouter()

logger = logging.getLogger(__name__)


@router.post("/spatial-filter/", response_model=SpatialFilterResponse)
async def get_links_in_spatial_filter(filter_request: SpatialFilterIn):
    """
    Get links with LINESTRING geometries within a geographic bounding box for a specific day and time period.
    
    Args:
        filter_request: Spatial filter request containing day, period, and bounding box
        
    Returns:
        SpatialFilterResponse: List of links with geometries and metadata
        
    Example Request Body:
        {
            "day": "Tuesday",
            "period": "AM Peak", 
            "bbox": [-81.8, 30.1, -81.6, 30.3]
        }
        
    Example Response:
        {
            "links": [
                {
                    "link_id": 1148855686,
                    "road_name": "Main Street",
                    "geometry": "LINESTRING(-81.75 30.2, -81.74 30.21)",
                    "average_speed": 35.5
                }
            ],
            "count": 1,
            "day_of_week": 3,
            "period": 3,
            "bbox": [-81.8, 30.1, -81.6, 30.3]
        }
        
    Day Mapping:
        Sunday = 1
        Monday = 2
        Tuesday = 3
        Wednesday = 4
        Thursday = 5
        Friday = 6
        Saturday = 7
        
    Period Mapping:
        Overnight = 1
        Early Morning = 2
        AM Peak = 3
        Midday = 4
        Early Afternoon = 5
        PM Peak = 6
        Evening = 7
    """
    try:
        # Get numeric values from the request
        day_number = filter_request.get_day_number()
        period_number = filter_request.get_period_number()
        west, south, east, north = filter_request.get_bbox_tuple()
        
        logger.info(f"Spatial filter request: day={filter_request.day} ({day_number}), "
                   f"period={filter_request.period} ({period_number}), "
                   f"bbox=[{west}, {south}, {east}, {north}]")
        
        # Call the service function to get links with geometries
        links_data = await get_links_with_geometries_in_box_day_period(
            west, south, east, north, day_number, period_number
        )
        
        logger.info(f"Found {len(links_data)} links in spatial filter")
        
        # Convert service response to LinkGeometry models
        links = [
            LinkGeometry(
                link_id=link["link_id"],
                road_name=link["road_name"],
                geometry=link["geometry"],
                average_speed=link["average_speed"]
            )
            for link in links_data
        ]
        
        # Build response
        response = SpatialFilterResponse(
            links=links,
            count=len(links),
            day_of_week=day_number,
            period=period_number,
            bbox=filter_request.bbox
        )
        
        return response
        
    except ValueError as e:
        # This should be caught by pydantic validation, but just in case
        logger.error(f"Validation error in spatial filter: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Validation error: {str(e)}")
    except Exception as e:
        logger.error(f"Error in spatial filter: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

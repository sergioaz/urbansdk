import logging
from fastapi import APIRouter, HTTPException, Depends
from typing import List
from app.models.aggregate import AggregateRequest, LinkData
from app.services.aggregate import get_links_geometry_roadname_speed_by_day_period

router = APIRouter()

logger = logging.getLogger(__name__)

@router.get("/aggregates/", response_model=List[LinkData])
async def get_aggregated_speed(request: AggregateRequest = Depends()):
    """
    Get links with link_id, geometry, road_name, and average_speed for the given day and time period.
    
    Args:
        request: AggregateRequest containing day and period from query parameters
        
    Returns:
        List[LinkData]: Array of links with link_id, geometry, road_name, and average_speed
        
    Example API Calls:
        GET /aggregates/?day=Tuesday&period=AM%20Peak
        GET /aggregates/?day=Monday&period=Evening
        GET /aggregates/?day=Friday&period=Overnight
        
    Example Response:
        [
            {
                "link_id": 12345,
                "geometry": "LINESTRING(-81.51023 30.16599, -81.51038 30.16637)",
                "road_name": "Main Street",
                "average_speed": 41.5
            },
            {
                "link_id": 12346,
                "geometry": "LINESTRING(-81.51040 30.16640, -81.51055 30.16678)",
                "road_name": "Oak Avenue",
                "average_speed": 38.2
            }
        ]
        
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
        day_number = request.get_day_number()
        period_number = request.get_period_number()
        
        logger.info(f"Aggregate request: day={request.day} ({day_number}), "
                   f"period={request.period} ({period_number})")
        
        # Call the service function to get simplified data
        links_data = await get_links_geometry_roadname_speed_by_day_period(day_number, period_number)
        
        # Check if data was found
        if not links_data:
            raise HTTPException(
                status_code=404, 
                detail=f"No data found for day '{request.day}' ({day_number}) and period '{request.period}' ({period_number})"
            )
        
        logger.info(f"Found {len(links_data)} links for aggregate data")
        
        # Convert to LinkData models and return array directly
        links = [
            LinkData(
                link_id=link["link_id"],
                geometry=link["geometry"],
                road_name=link["road_name"],
                average_speed=link["average_speed"]
            )
            for link in links_data
        ]
        
        return links
        
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except ValueError as e:
        # This should be caught by pydantic validation, but just in case
        logger.error(f"Validation error in aggregate request: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Validation error: {str(e)}")
    except Exception as e:
        logger.error(f"Error in aggregate request: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

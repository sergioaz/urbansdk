import logging
from fastapi import APIRouter, HTTPException, Depends
from typing import List
from app.models.slow_links import SlowLinksRequest, LinkData
from app.services.aggregate import get_slow_links_period_threshold_min_days

router = APIRouter()

logger = logging.getLogger(__name__)

@router.get("/slow_links/", response_model=List[LinkData])
async def get_slow_links(request: SlowLinksRequest = Depends()):
    """
    Get links with link_id, geometry, road_name, and overall_average_speed for the given time period
    with average speed below threshold for at least min_days in a week
    
    Args:
        request: SlowLinksRequest containing period, threshold and min_days from query parameters
        
    Returns:
        List[LinkData]: Array of links with link_id, geometry, road_name, and average_speed
        
    Example API Calls:
        GET /slow_links/?period=AM%20Peak&threshold=50.5&min_days=7
        GET /slow_links/?period=Evening&threshold=30.5&min_days=2
        
    Example Response:
        [
            {
                "link_id": 12345,
                "geometry": "LINESTRING(-81.51023 30.16599, -81.51038 30.16637)",
                "road_name": "Main Street",
                "overall_average_speed": 41.5
            },
            {
                "link_id": 12346,
                "geometry": "LINESTRING(-81.51040 30.16640, -81.51055 30.16678)",
                "road_name": "Oak Avenue",
                "overall_average_speed": 38.2
            }
        ]

    Period Mapping:
        Overnight = 1
        Early Morning = 2
        AM Peak = 3
        Midday = 4
        Early Afternoon = 5
        PM Peak = 6
        Evening = 7
    """
    print (f"In get_slow_links, {request=} ")
    try:
        # Get numeric values from the request
        period_number = request.get_period_number()
        
        logger.info(f"Aggregate request: threshold={request.threshold}, min_days={request.min_days}, period={request.period} ({period_number})")
        
        # Call the service function to get simplified data
        links_data = await get_slow_links_period_threshold_min_days(period_number, request.threshold, request.min_days)
        
        # Check if data was found
        if not links_data:
            raise HTTPException(
                status_code=404, 
                detail=f"No data found for period '{request.period}' ({period_number}), threshold {request.threshold} and min_days {request.min_days}"
            )
        
        logger.info(f"Found {len(links_data)} links for aggregate data")
        
        # Convert to LinkData models and return array directly
        links = [
            LinkData(
                link_id=link["link_id"],
                geometry=link["geometry"],
                road_name=link["road_name"],
                overall_average_speed=link["overall_average_speed"]
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

import logging
from fastapi import APIRouter, HTTPException, Depends
from app.services.aggregate import get_average_speed_by_link_day_period
from app.models.aggregate_link import AggregateLinkResponse, AggregateLinkRequest

router = APIRouter()

logger = logging.getLogger(__name__)

@router.get("/aggregates/{link_id}", response_model=AggregateLinkResponse)
async def get_aggregate_link_data(
    link_id: int,
    request: AggregateLinkRequest = Depends()
):
    """
    Get speed and metadata for a single road segment.

    Args:
        link_id: Unique identifier for the road segment
        request: AggregateLinkRequest containing day and period from query parameters

    Returns:
        Aggregated speed and metadata for the specified link
        
    Example API Calls:
        GET /aggregates/1148855686?day=Tuesday&period=AM%20Peak
        GET /aggregates/1240632857?day=Monday&period=Evening
        GET /aggregates/1240632858?day=Friday&period=Overnight
        
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
        
        logger.info(f"Aggregate link request: link_id={link_id}, day={request.day} ({day_number}), "
                   f"period={request.period} ({period_number})")
        
        # Call the service function
        result = await get_average_speed_by_link_day_period(link_id, day_number, period_number)

        # Check if data was found
        if "error" in result:
            raise HTTPException(
                status_code=404,
                detail=f"No data found for link {link_id}, day '{request.day}' ({day_number}), and period '{request.period}' ({period_number})"
            )

        logger.info(f"Found aggregate link data: link_id={link_id}, average_speed={result['average_speed']}")

        return AggregateLinkResponse(**result)

    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except ValueError as e:
        # This should be caught by pydantic validation, but just in case
        logger.error(f"Validation error in aggregate link request: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Validation error: {str(e)}")
    except Exception as e:
        logger.error(f"Error in aggregate link request: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

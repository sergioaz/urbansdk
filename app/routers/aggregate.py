import logging
from fastapi import APIRouter, HTTPException, Depends
from app.models.aggregate import AggregateRequest, AggregateResponse
from app.services.aggregate import get_average_speed_by_day_period

router = APIRouter()

logger = logging.getLogger(__name__)

@router.get("/aggregates/", response_model=AggregateResponse)
async def get_aggregated_speed(request: AggregateRequest = Depends()):
    """
    Get aggregated average speed for the given day and time period.
    
    Args:
        request: AggregateRequest containing day and period from query parameters
        
    Returns:
        AggregateResponse: Aggregated data with average speed
        
    Example API Calls:
        GET /aggregates/?day=Tuesday&period=AM%20Peak
        GET /aggregates/?day=Monday&period=Evening
        GET /aggregates/?day=Friday&period=Overnight
        
    Example Response:
        {
            "day_of_week": 3,
            "period": 3,
            "average_speed": 42.75
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
        day_number = request.get_day_number()
        period_number = request.get_period_number()
        
        logger.info(f"Aggregate request: day={request.day} ({day_number}), "
                   f"period={request.period} ({period_number})")
        
        # Call the service function
        result = await get_average_speed_by_day_period(day_number, period_number)
        
        # Check if data was found
        if result["average_speed"] == 0.0:
            raise HTTPException(
                status_code=404, 
                detail=f"No data found for day '{request.day}' ({day_number}) and period '{request.period}' ({period_number})"
            )
        
        logger.info(f"Found aggregate data: average_speed={result['average_speed']}")
        
        return AggregateResponse(**result)
        
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

import logging
from fastapi import APIRouter, Query, HTTPException
from app.models.aggregate import AggregateResponse
from app.services.aggregate import get_average_speed_by_day_period
from app.helpers.periods import PERIOD_MAPPING, PeriodName, get_valid_periods

router = APIRouter()

logger = logging.getLogger(__name__)

@router.get("/aggregates/", response_model=AggregateResponse)
async def get_aggregated_speed(
    day: int = Query(..., description="Day of week (1-7, where 1=Monday)", ge=1, le=7),
    period: PeriodName = Query(..., description="Time period name")
):
    """
    Get aggregated average speed for the given day and time period.
    
    Args:
        day: Day of week (1-7, where 1=Monday)
        period: Time period name (Overnight, Early Morning, AM Peak, Midday, Early Afternoon, PM Peak, Evening)
        
    Returns:
        AggregateResponse: Aggregated data with average speed
        
    Example API Calls:
        GET /aggregates/?day=2&period=AM Peak
        GET /aggregates/?day=1&period=Evening
        GET /aggregates/?day=5&period=Overnight
        
    Example Response:
        {
            "day_of_week": 2,
            "period": 3,
            "average_speed": 42.75
        }
        
    Day Mapping:
        1 = Monday
        2 = Tuesday
        3 = Wednesday
        4 = Thursday
        5 = Friday
        6 = Saturday
        7 = Sunday
        
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
        # Translate period name to number
        period_number = PERIOD_MAPPING[period]
        
        # Call the service function
        result = await get_average_speed_by_day_period(day, period_number)
        
        # Check if data was found
        if result["average_speed"] == 0.0:
            raise HTTPException(
                status_code=404, 
                detail=f"No data found for day {day} and period '{period}' ({period_number})"
            )
        
        return AggregateResponse(**result)
        
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except KeyError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid period name '{period}'. Valid options: {list(PERIOD_MAPPING.keys())}"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

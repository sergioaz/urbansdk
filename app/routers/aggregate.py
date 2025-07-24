import logging
from fastapi import APIRouter, Query, HTTPException
from app.models.aggregate import AggregateResponse
from app.services.aggregate import get_average_speed_by_day_period
from app.helpers.periods import PERIOD_MAPPING, PeriodName, get_valid_periods, DAY_MAPPING, DayName

router = APIRouter()

logger = logging.getLogger(__name__)

@router.get("/aggregates/", response_model=AggregateResponse)
async def get_aggregated_speed(
    day: DayName = Query(..., description="Day of week name"),
    period: PeriodName = Query(..., description="Time period name")
):
    """
    Get aggregated average speed for the given day and time period.
    
    Args:
        day: Day of week name (Sunday, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday)
        period: Time period name (Overnight, Early Morning, AM Peak, Midday, Early Afternoon, PM Peak, Evening)
        
    Returns:
        AggregateResponse: Aggregated data with average speed
        
    Example API Calls:
        GET /aggregates/?day=Tuesday&period=AM Peak
        GET /aggregates/?day=Monday&period=Evening
        GET /aggregates/?day=Friday&period=Overnight
        
    Example Response:
        {
            "day_of_week": 2,
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
        # Translate day name to number
        day_number = DAY_MAPPING[day]
        
        # Translate period name to number
        period_number = PERIOD_MAPPING[period]
        
        # Call the service function
        result = await get_average_speed_by_day_period(day_number, period_number)
        
        # Check if data was found
        if result["average_speed"] == 0.0:
            raise HTTPException(
                status_code=404, 
                detail=f"No data found for day '{day}' ({day_number}) and period '{period}' ({period_number})"
            )
        
        return AggregateResponse(**result)
        
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except KeyError as e:
        # Handle both day and period key errors
        if str(e).strip("'") in DAY_MAPPING:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid day name '{day}'. Valid options: {list(DAY_MAPPING.keys())}"
            )
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid period name '{period}'. Valid options: {list(PERIOD_MAPPING.keys())}"
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

from fastapi import APIRouter, HTTPException, Query
from app.services.aggregate import get_average_speed_by_link_day_period
from app.models.aggregate_link import AggregateLinkResponse
from app.helpers.periods import PERIOD_MAPPING, PeriodName, get_valid_periods, DAY_MAPPING, DayName

router = APIRouter()

@router.get("/aggregates/{link_id}", response_model=AggregateLinkResponse)
async def get_aggregate_link_data(
    link_id: int,
    day: DayName = Query(..., description="Day of week name"),
    period: PeriodName = Query(..., description="Time period name")
):
    """
    Get speed and metadata for a single road segment.

    Args:
        link_id: Unique identifier for the road segment
        day: Day of week name (Sunday, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday)
        period: Time period name (Overnight, Early Morning, AM Peak, Midday, Early Afternoon, PM Peak, Evening)

    Returns:
        Aggregated speed and metadata for the specified link
        
    Example API Calls:
        GET /aggregates/1148855686?day=Tuesday&period=AM Peak
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
        # Translate day name to number
        day_number = DAY_MAPPING[day]
        
        # Translate period name to number
        period_number = PERIOD_MAPPING[period]
        
        # Call the service function
        result = await get_average_speed_by_link_day_period(link_id, day_number, period_number)

        # Check if data was found
        if "error" in result:
            raise HTTPException(
                status_code=404,
                detail=f"No data found for link {link_id}, day '{day}' ({day_number}), and period '{period}' ({period_number})"
            )

        return AggregateLinkResponse(**result)

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
                detail=f"Invalid period name '{period}'. Valid options: {get_valid_periods()}"
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

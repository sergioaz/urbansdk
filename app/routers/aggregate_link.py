from fastapi import APIRouter, HTTPException, Query
from app.services.aggregate import get_average_speed_by_link_day_period
from app.models.aggregate_link import AggregateLinkResponse
from app.helpers.periods import PERIOD_MAPPING, PeriodName, get_valid_periods

router = APIRouter()

@router.get("/aggregates/{link_id}", response_model=AggregateLinkResponse)
async def get_aggregate_link_data(
    link_id: int,
    day: int = Query(..., description="Day of week (1-7, where 1=Monday)", ge=1, le=7),
    period: PeriodName = Query(..., description="Time period name")
):
    """
    Get speed and metadata for a single road segment.

    Args:
        link_id: Unique identifier for the road segment
        day: Day of week (1-7, where 1=Monday)
        period: Time period name (Overnight, Early Morning, AM Peak, Midday, Early Afternoon, PM Peak, Evening)

    Returns:
        Aggregated speed and metadata for the specified link
        
    Example API Calls:
        GET /aggregates/1148855686?day=2&period=AM Peak
        GET /aggregates/1240632857?day=1&period=Evening
        GET /aggregates/1240632858?day=5&period=Overnight
        
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
        result = await get_average_speed_by_link_day_period(link_id, day, period_number)

        # Check if data was found
        if "error" in result:
            raise HTTPException(
                status_code=404,
                detail=f"No data found for link {link_id}, day {day}, and period '{period}' ({period_number})"
            )

        return AggregateLinkResponse(**result)

    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except KeyError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid period name '{period}'. Valid options: {get_valid_periods()}"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

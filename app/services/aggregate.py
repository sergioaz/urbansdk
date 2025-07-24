from typing import List, Dict
from sqlalchemy import select, func
from app.db.database import database, duval_table


async def get_average_speed_by_day_period(day: int, period: int) -> Dict:
    """
    Calculate aggregated average speed for the given day and time period.
    
    Args:
        day (int): Day of week (1-7, where 1=Monday)
        period (int): Time period identifier
        
    Returns:
        Dict: Aggregated data with average speed for the day and period
        
    Raises:
        Exception: If database query fails
    """
    try:
        # Build the query to aggregate average speed by day_of_week and period
        query = (
            select(
                duval_table.c.day_of_week,
                duval_table.c.period,
                func.avg(duval_table.c.average_speed).label("average_speed")
            )
            .where(
                (duval_table.c.day_of_week == day) & 
                (duval_table.c.period == period)
            )
            .group_by(
                duval_table.c.day_of_week,
                duval_table.c.period
            )
        )
        
        # Execute the query
        result = await database.fetch_one(query)
        
        if result:
            return {
                "day_of_week": result["day_of_week"],
                "period": result["period"],
                "average_speed": round(float(result["average_speed"]) if result["average_speed"] else 0, 2)
            }
        else:
            return {
                "day_of_week": day,
                "period": period,
                "average_speed": 0.0
            }
        
    except Exception as e:
        raise Exception(f"Failed to calculate average speed aggregation: {str(e)}")

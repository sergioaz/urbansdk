from typing import List, Dict
from sqlalchemy import select, func
from app.db.database import database, duval_table, link_info_table


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
    #TODO: Make date and period optional, exclude from query if not provided
    if day < 1 or day > 7:
        raise ValueError("Day must be between 1 and 7 (1=Monday, 7=Sunday)")
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


async def get_average_speed_by_link_day_period(link_id: int, day: int, period: int) -> Dict:
    """
    Calculate aggregated average speed for the given link, day and time period with metadata.

    Args:
        link_id (int): Link identifier
        day (int): Day of week (1-7, where 1=Monday)
        period (int): Time period identifier

    Returns:
        Dict: Aggregated data with average speed and link metadata

    Raises:
        Exception: If database query fails
    """
    #TODO: Make date and period optional, exclude from query if not provided

    try:
        # Build the query with JOIN to get speed data and metadata
        query = (
            select(
                duval_table.c.link_id,
                duval_table.c.day_of_week,
                duval_table.c.period,
                func.avg(duval_table.c.average_speed).label("average_speed"),
                func.avg(duval_table.c.freeflow).label("average_freeflow"),
                func.count(duval_table.c.link_id).label("record_count"),
                # Link metadata from link_info table
                link_info_table.c.length,
                link_info_table.c.road_name,
                link_info_table.c.usdk_speed_category,
                link_info_table.c.funclass_id,
                link_info_table.c.speedcat,
                link_info_table.c.volume_value,
                link_info_table.c.volume_bin_id,
                link_info_table.c.volume_year,
                link_info_table.c.volumes_bin_description,
                link_info_table.c.geo_json
            )
            .select_from(
                duval_table.join(
                    link_info_table,
                    duval_table.c.link_id == link_info_table.c.link_id
                )
            )
            .where(
                (duval_table.c.link_id == link_id) &
                (duval_table.c.day_of_week == day) &
                (duval_table.c.period == period)
            )
            .group_by(
                duval_table.c.link_id,
                duval_table.c.day_of_week,
                duval_table.c.period,
                link_info_table.c.length,
                link_info_table.c.road_name,
                link_info_table.c.usdk_speed_category,
                link_info_table.c.funclass_id,
                link_info_table.c.speedcat,
                link_info_table.c.volume_value,
                link_info_table.c.volume_bin_id,
                link_info_table.c.volume_year,
                link_info_table.c.volumes_bin_description,
                link_info_table.c.geo_json
            )
        )

        # Execute the query
        result = await database.fetch_one(query)

        if result:
            return {
                "link_id": result["link_id"],
                "day_of_week": result["day_of_week"],
                "period": result["period"],
                "average_speed": round(float(result["average_speed"]) if result["average_speed"] else 0, 2),
                "average_freeflow": round(float(result["average_freeflow"]) if result["average_freeflow"] else 0, 2),
                "record_count": result["record_count"],
                # Link metadata
                "length": round(float(result["length"]) if result["length"] else 0, 6),
                "road_name": result["road_name"],
                "usdk_speed_category": result["usdk_speed_category"],
                "funclass_id": result["funclass_id"],
                "speedcat": result["speedcat"],
                "volume_value": result["volume_value"],
                "volume_bin_id": result["volume_bin_id"],
                "volume_year": result["volume_year"],
                "volumes_bin_description": result["volumes_bin_description"],
                "geo_json": result["geo_json"]
            }
        else:
            return {
                "link_id": link_id,
                "day_of_week": day,
                "period": period,
                "average_speed": 0.0,
                "error": "No data found for the specified link, day, and period"
            }

    except Exception as e:
        raise Exception(f"Failed to calculate average speed aggregation for link {link_id}: {str(e)}")

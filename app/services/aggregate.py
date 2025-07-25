from typing import List, Dict
from sqlalchemy import select, func, text
from geoalchemy2.functions import ST_AsText
from app.db.database import database, speed_record_table, link_table

duval_table = speed_record_table
link_info_table = link_table


async def get_average_speed_by_day_period(day: int, period: int) -> Dict:
    """
    Calculate aggregated average speed for the given day and time period,
    including individual link data with road names and geometries.
    
    Args:
        day (int): Day of week (1-7, where 1=Monday)
        period (int): Time period identifier
        
    Returns:
        Dict: Aggregated data with average speed, record count, and list of links
        
    Raises:
        Exception: If database query fails
    """
    if day < 1 or day > 7:
        raise ValueError("Day must be between 1 and 7 (1=Monday, 7=Sunday)")
    
    try:
        # Build the query to get individual link data with road names and geometries
        query = (
            select(
                duval_table.c.link_id,
                duval_table.c.day_of_week,
                duval_table.c.period,
                func.avg(duval_table.c.average_speed).label("link_average_speed"),
                func.count(duval_table.c.link_id).label("link_record_count"),
                link_info_table.c.road_name,
                ST_AsText(link_info_table.c.geometry).label("geometry_wkt")
            )
            .select_from(
                duval_table.join(
                    link_info_table,
                    duval_table.c.link_id == link_info_table.c.link_id
                )
            )
            .where(
                (duval_table.c.day_of_week == day) & 
                (duval_table.c.period == period)
            )
            .group_by(
                duval_table.c.link_id,
                duval_table.c.day_of_week,
                duval_table.c.period,
                link_info_table.c.road_name,
                link_info_table.c.geometry
            )
            .order_by(duval_table.c.link_id)
        )
        
        # Execute the query to get all link data
        results = await database.fetch_all(query)
        
        if results:
            # Calculate overall statistics
            total_speed = sum(float(row["link_average_speed"]) for row in results)
            total_records = sum(int(row["link_record_count"]) for row in results)
            overall_average = total_speed / len(results) if results else 0.0
            
            # Build links list with road names and geometries
            links = []
            for row in results:
                links.append({
                    "link_id": row["link_id"],
                    "road_name": row["road_name"],
                    "geometry": row["geometry_wkt"] if row["geometry_wkt"] else None,
                    "average_speed": round(float(row["link_average_speed"]) if row["link_average_speed"] else 0, 2)
                })
            
            return {
                "day_of_week": results[0]["day_of_week"],
                "period": results[0]["period"],
                "average_speed": round(overall_average, 2),
                "record_count": total_records,
                "links": links
            }
        else:
            return {
                "day_of_week": day,
                "period": period,
                "average_speed": 0.0,
                "record_count": 0,
                "links": []
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
                func.count(duval_table.c.link_id).label("record_count"),
                # Link metadata from link_info table
                link_info_table.c.road_name,
                link_info_table.c.geometry
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
                link_info_table.c.road_name,
                link_info_table.c.geometry
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
                "record_count": result["record_count"],
                "road_name": result["road_name"],
                "geometry": result["geometry"]
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


async def get_link_in_box_day_period(west: float, south: float, east: float, north: float, day: int, period: int) -> List[int]:
    """
    Retrieves link IDs within a geographic bounding box for a given day and time period.

    Args:
        west (float): Western longitude of the bounding box
        south (float): Southern latitude of the bounding box
        east (float): Eastern longitude of the bounding box
        north (float): Northern latitude of the bounding box
        day (int): Day of week (1-7, where 1=Monday)
        period (int): Time period identifier

    Returns:
        List[int]: List of link IDs that are inside the bounding box on the specified day and period

    Raises:
        Exception: If database query fails or invalid arguments are provided
    """
    # Validate input arguments
    if day < 1 or day > 7:
        raise ValueError("Day must be between 1 and 7 (1=Monday, 7=Sunday)")
    
    try:
        # Create a query to retrieve distinct link IDs in the bounding box, day, and time period
        query = (
            select(duval_table.c.link_id)
            .select_from(
                duval_table.join(
                    link_info_table,
                    duval_table.c.link_id == link_info_table.c.link_id
                )
            )
            .where(
                text(
                    f"ST_Intersects("
                    f"geometry, "
                    f"ST_MakeEnvelope({west}, {south}, {east}, {north}, 4326)"
                    f")"
                ) &
                (duval_table.c.day_of_week == day) &
                (duval_table.c.period == period)
            )
            .distinct()
        )

        # Execute the query
        result_set = await database.fetch_all(query)

        # Extract and return link IDs from the result
        return [row["link_id"] for row in result_set]

    except Exception as e:
        raise Exception(f"Failed to retrieve links in box by day and period: {str(e)}")


async def get_links_with_geometries_in_box_day_period(
    west: float, south: float, east: float, north: float, day: int, period: int
) -> List[Dict]:
    """
    Retrieves links with their geometries within a geographic bounding box for a given day and time period.

    Args:
        west (float): Western longitude of the bounding box
        south (float): Southern latitude of the bounding box
        east (float): Eastern longitude of the bounding box
        north (float): Northern latitude of the bounding box
        day (int): Day of week (1-7, where 1=Monday)
        period (int): Time period identifier

    Returns:
        List[Dict]: List of dictionaries containing link_id, road_name, and geometry (WKT)

    Raises:
        Exception: If database query fails or invalid arguments are provided
    """
    # Validate input arguments
    if day < 1 or day > 7:
        raise ValueError("Day must be between 1 and 7 (1=Monday, 7=Sunday)")
    
    try:
        # Create a query to retrieve link data with geometries in the bounding box
        query = (
            select(
                duval_table.c.link_id,
                link_info_table.c.road_name,
                ST_AsText(link_info_table.c.geometry).label("geometry_wkt"),
                func.avg(duval_table.c.average_speed).label("average_speed")
            )
            .select_from(
                duval_table.join(
                    link_info_table,
                    duval_table.c.link_id == link_info_table.c.link_id
                )
            )
            .where(
                text(
                    f"ST_Intersects("
                    f"geometry, "
                    f"ST_MakeEnvelope({west}, {south}, {east}, {north}, 4326)"
                    f")"
                ) &
                (duval_table.c.day_of_week == day) &
                (duval_table.c.period == period)
            )
            .group_by(
                duval_table.c.link_id,
                link_info_table.c.road_name,
                link_info_table.c.geometry
            )
            .distinct()
        )

        # Execute the query
        result_set = await database.fetch_all(query)

        # Build list of links with their geometries
        links = []
        for row in result_set:
            links.append({
                "link_id": row["link_id"],
                "road_name": row["road_name"],
                "geometry": row["geometry_wkt"] if row["geometry_wkt"] else None,
                "average_speed": round(float(row["average_speed"]) if row["average_speed"] else 0, 2)
            })

        return links

    except Exception as e:
        raise Exception(f"Failed to retrieve links with geometries in box by day and period: {str(e)}")


async def get_links_geometry_roadname_speed_by_day_period(day: int, period: int) -> List[Dict]:
    """
    Get links with only geometry, road_name, and average_speed for the given day and time period.
    
    Args:
        day (int): Day of week (1-7, where 1=Monday)
        period (int): Time period identifier
        
    Returns:
        List[Dict]: List of links with geometry, road_name, and average_speed only
        
    Raises:
        Exception: If database query fails
    """
    if day < 1 or day > 7:
        raise ValueError("Day must be between 1 and 7 (1=Monday, 7=Sunday)")
    
    try:
        # Build the query to get individual link data with road names and geometries
        query = (
            select(
                duval_table.c.link_id,
                link_info_table.c.road_name,
                ST_AsText(link_info_table.c.geometry).label("geometry_wkt"),
                func.avg(duval_table.c.average_speed).label("link_average_speed")
            )
            .select_from(
                duval_table.join(
                    link_info_table,
                    duval_table.c.link_id == link_info_table.c.link_id
                )
            )
            .where(
                (duval_table.c.day_of_week == day) & 
                (duval_table.c.period == period)
            )
            .group_by(
                duval_table.c.link_id,
                link_info_table.c.road_name,
                link_info_table.c.geometry
            )
            .order_by(duval_table.c.link_id)
        )
        
        # Execute the query to get all link data
        results = await database.fetch_all(query)
        
        # Build simplified links list with only required fields
        links = []
        for row in results:
            links.append({
                "link_id": row["link_id"],
                "geometry": row["geometry_wkt"] if row["geometry_wkt"] else None,
                "road_name": row["road_name"],
                "average_speed": round(float(row["link_average_speed"]) if row["link_average_speed"] else 0, 2)
            })
        
        return links
        
    except Exception as e:
        raise Exception(f"Failed to get simplified aggregate data: {str(e)}")

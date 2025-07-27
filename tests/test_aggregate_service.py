import pytest
from decimal import Decimal
from app.services.aggregate import (get_average_speed_by_day_period, get_average_speed_by_link_day_period,
                                    get_link_in_box_day_period, get_slow_links_period_threshold_min_days,
                                    get_links_geometry_roadname_speed_by_day_period)
from app.db.database import database


class Test_get_average_speed_by_day_period:
    """Test cases for aggregate service functions using live PostgreSQL connection"""
    
    #@pytest.fixture(autouse=True)
    async def setup_and_teardown(self):
        """Setup and teardown fixture for each test method
        Does not really work for async tests, but kept for structure.
        """
        # Setup: Connect to database
        if not database.is_connected:
            await database.connect()
        
        yield  # This is where the test runs
        
        # Teardown: Keep connection for other tests (optional cleanup)
        # We'll keep the connection alive to avoid reconnection overhead

    @pytest.mark.asyncio
    async def test_get_average_speed_valid_data(self):
        """Test getting average speed with valid day and period that should have data"""
        # Use day=2 (Tuesday) and period=4 which should have data based on your sample

        # connect to the database
        if not database.is_connected:
            await database.connect()

        day = 2
        period = 4


        result = await get_average_speed_by_day_period(day, period)
        await database.disconnect()  # Disconnect after test

        # Assertions
        assert isinstance(result, dict)
        assert "day_of_week" in result
        assert "period" in result
        assert "average_speed" in result
        
        assert result["day_of_week"] == day
        assert result["period"] == period
        assert isinstance(result["average_speed"], float)
        assert result["average_speed"] >= 0.0

    @pytest.mark.asyncio
    async def test_get_average_speed_no_data(self):
        """Test getting average speed with day and period that have no data"""
        # Use day=7 (Sunday) and period=999 which should have no data

        # connect to the database
        if not database.is_connected:
            await database.connect()

        day = 7
        period = 999

        result = await get_average_speed_by_day_period(day, period)

        await database.disconnect()  # Disconnect after test
        
        # Assertions for no data case
        assert isinstance(result, dict)
        assert result["day_of_week"] == day
        assert result["period"] == period
        assert result["average_speed"] == 0.0

    @pytest.mark.asyncio
    async def test_get_average_speed_monday_period_1(self):
        """Test getting average speed for Monday period 1"""

        # connect to the database
        if not database.is_connected:
            await database.connect()

        day = 1  # Monday
        period = 1

        result = await get_average_speed_by_day_period(day, period)

        await database.disconnect()  # Disconnect after test

        assert result["day_of_week"] == 1
        assert result["period"] == 1
        assert isinstance(result["average_speed"], float)

    @pytest.mark.asyncio
    async def test_get_average_speed_multiple_periods(self):
        """Test getting average speed for multiple periods to ensure data consistency"""
        # connect to the database
        if not database.is_connected:
            await database.connect()

        day = 2  # Tuesday
        periods = [1, 2, 3, 4, 5]
        
        results = []
        for period in periods:
            result = await get_average_speed_by_day_period(day, period)
            results.append(result)
            
            # Basic assertions for each result
            assert result["day_of_week"] == day
            assert result["period"] == period
            assert isinstance(result["average_speed"], float)
            assert result["average_speed"] >= 0.0

        # Check that we got results for all periods
        assert len(results) == len(periods)

        await database.disconnect()  # Disconnect after test


    @pytest.mark.asyncio
    async def test_get_average_speed_all_weekdays(self):
        """Test getting average speed for all weekdays with a common period"""
        # connect to the database
        if not database.is_connected:
            await database.connect()

        period = 4  # Use period 4 which should have data
        weekdays = [1, 2, 3, 4, 5]  # Monday through Friday
        
        results = []
        for day in weekdays:
            result = await get_average_speed_by_day_period(day, period)
            results.append(result)
            
            # Basic assertions
            assert result["day_of_week"] == day
            assert result["period"] == period
            assert isinstance(result["average_speed"], float)

        # Verify we got results for all weekdays
        assert len(results) == len(weekdays)

        await database.disconnect()  # Disconnect after test


    @pytest.mark.asyncio
    async def test_average_speed_precision(self):
        """Test that average speed is rounded to 2 decimal places"""
        # connect to the database
        if not database.is_connected:
            await database.connect()

        day = 2
        period = 4
        
        result = await get_average_speed_by_day_period(day, period)
        
        # Check precision (should be rounded to 2 decimal places)
        speed_str = str(result["average_speed"])
        if '.' in speed_str:
            decimal_places = len(speed_str.split('.')[1])
            assert decimal_places <= 2, f"Average speed should be rounded to 2 decimal places, got {decimal_places}"

        await database.disconnect()  # Disconnect after test


    @pytest.mark.asyncio
    async def test_error_handling_invalid_database(self):
        """Test error handling when database query fails"""
        # connect to the database
        if not database.is_connected:
            await database.connect()

        # Temporarily disconnect database to simulate error
        await database.disconnect()
        
        with pytest.raises(Exception) as exc_info:
            await get_average_speed_by_day_period(1, 1)
        
        assert "Failed to calculate average speed aggregation" in str(exc_info.value)
        
        await database.disconnect()  # Disconnect after test


    @pytest.mark.asyncio
    async def test_edge_case_boundary_values(self):
        """Test boundary values for day and period"""
        # connect to the database
        if not database.is_connected:
            await database.connect()

        # Test minimum values
        result_min = await get_average_speed_by_day_period(1, 1)
        assert result_min["day_of_week"] == 1
        assert result_min["period"] == 1
        
        # Test maximum day value
        result_max_day = await get_average_speed_by_day_period(7, 1)
        assert result_max_day["day_of_week"] == 7
        assert result_max_day["period"] == 1

        await database.disconnect()  # Disconnect after test

    """Additional test cases for get_average_speed_by_day_period function"""

    @pytest.mark.asyncio
    async def test_data_consistency(self):
        """Test that repeated calls return consistent results"""
        # connect to the database
        if not database.is_connected:
            await database.connect()

        day = 2
        period = 4

        # Make multiple calls
        result1 = await get_average_speed_by_day_period(day, period)
        result2 = await get_average_speed_by_day_period(day, period)
        result3 = await get_average_speed_by_day_period(day, period)

        # Results should be identical
        assert result1 == result2 == result3

        await database.disconnect()  # Disconnect after test


class Test_get_average_speed_by_link_day_period:
    """Test cases for get_average_speed_by_link_day_period function using live PostgreSQL connection"""
    
    @pytest.mark.asyncio
    async def test_get_average_speed_by_link_valid_data(self):
        """Test getting average speed with valid link_id, day and period that should have data"""
        # Connect to database
        if not database.is_connected:
            await database.connect()
        
        # Use link_id from your data, day=2 (Tuesday) and period=4
        link_id = 1148855686  # From your sample data
        day = 2
        period = 4
        
        result = await get_average_speed_by_link_day_period(link_id, day, period)
        await database.disconnect()
        
        # Assertions (only fields that exist in our current schema)
        assert isinstance(result, dict)
        assert "link_id" in result
        assert "day_of_week" in result
        assert "period" in result
        assert "average_speed" in result
        assert "record_count" in result
        assert "road_name" in result
        assert "geometry" in result
        
        assert result["link_id"] == link_id
        assert result["day_of_week"] == day
        assert result["period"] == period
        assert isinstance(result["average_speed"], float)
        assert result["average_speed"] >= 0.0
        assert isinstance(result["record_count"], int)
        assert result["record_count"] >= 0
    
    @pytest.mark.asyncio
    async def test_get_average_speed_by_link_no_data(self):
        """Test getting average speed with link_id, day and period that have no data"""
        # Connect to database
        if not database.is_connected:
            await database.connect()
        
        # Use non-existent link_id and period
        link_id = 999999999
        day = 7
        period = 999
        
        result = await get_average_speed_by_link_day_period(link_id, day, period)
        await database.disconnect()
        
        # Assertions for no data case
        assert isinstance(result, dict)
        assert result["link_id"] == link_id
        assert result["day_of_week"] == day
        assert result["period"] == period
        assert result["average_speed"] == 0.0
        assert "error" in result
    
    @pytest.mark.asyncio
    async def test_get_average_speed_by_link_metadata_structure(self):
        """Test that all expected metadata fields are present in the response"""
        # Connect to database
        if not database.is_connected:
            await database.connect()
        
        # Use a known link_id
        link_id = 1240632857  # From your sample data
        day = 2
        period = 4
        
        result = await get_average_speed_by_link_day_period(link_id, day, period)
        await database.disconnect()
        
        # Check all expected fields are present (only fields that exist in our current schema)
        expected_fields = [
            "link_id", "day_of_week", "period", "average_speed",
            "record_count", "road_name", "geometry"
        ]
        
        for field in expected_fields:
            assert field in result, f"Field '{field}' missing from result"
    
    @pytest.mark.asyncio
    async def test_get_average_speed_by_link_multiple_links(self):
        """Test getting average speed for multiple link_ids to ensure data consistency"""
        # Connect to database
        if not database.is_connected:
            await database.connect()
        
        # Use multiple known link_ids from your data
        link_ids = [1148855686, 1240632857, 1240632858]
        day = 2  # Tuesday
        period = 4
        
        results = []
        for link_id in link_ids:
            result = await get_average_speed_by_link_day_period(link_id, day, period)
            results.append(result)
            
            # Basic assertions for each result
            assert result["link_id"] == link_id
            assert result["day_of_week"] == day
            assert result["period"] == period
            assert isinstance(result["average_speed"], float)
        
        # Check that we got results for all link_ids
        assert len(results) == len(link_ids)
        await database.disconnect()
    
    @pytest.mark.asyncio
    async def test_get_average_speed_by_link_precision(self):
        """Test that average speed and other numeric fields are properly rounded"""
        # Connect to database
        if not database.is_connected:
            await database.connect()
        
        link_id = 1240632857
        day = 2
        period = 4
        
        result = await get_average_speed_by_link_day_period(link_id, day, period)
        await database.disconnect()
        
        # Check precision (should be rounded to 2 decimal places for speeds)
        if result["average_speed"] > 0:
            speed_str = str(result["average_speed"])
            if '.' in speed_str:
                decimal_places = len(speed_str.split('.')[1])
                assert decimal_places <= 2, f"Average speed should be rounded to 2 decimal places, got {decimal_places}"
        
        # Check length precision (should be rounded to 6 decimal places)
        if result.get("length", 0) > 0:
            length_str = str(result["length"])
            if '.' in length_str:
                decimal_places = len(length_str.split('.')[1])
                assert decimal_places <= 6, f"Length should be rounded to 6 decimal places, got {decimal_places}"
    
    @pytest.mark.asyncio
    async def test_get_average_speed_by_link_edge_cases(self):
        """Test edge cases and boundary values"""
        # Connect to database
        if not database.is_connected:
            await database.connect()
        
        # Test with minimum day and period values
        link_id = 1148855686
        result_min = await get_average_speed_by_link_day_period(link_id, 1, 1)
        assert result_min["link_id"] == link_id
        assert result_min["day_of_week"] == 1
        assert result_min["period"] == 1
        
        # Test with maximum day value
        result_max_day = await get_average_speed_by_link_day_period(link_id, 7, 1)
        assert result_max_day["link_id"] == link_id
        assert result_max_day["day_of_week"] == 7
        assert result_max_day["period"] == 1
        
        await database.disconnect()
    
    @pytest.mark.asyncio
    async def test_get_average_speed_by_link_data_consistency(self):
        """Test that repeated calls return consistent results"""
        # Connect to database
        if not database.is_connected:
            await database.connect()
        
        link_id = 1240632857
        day = 2
        period = 4
        
        # Make multiple calls
        result1 = await get_average_speed_by_link_day_period(link_id, day, period)
        result2 = await get_average_speed_by_link_day_period(link_id, day, period)
        result3 = await get_average_speed_by_link_day_period(link_id, day, period)
        
        # Results should be identical
        assert result1 == result2 == result3
        
        await database.disconnect()
    
    @pytest.mark.asyncio
    async def test_get_average_speed_by_link_error_handling(self):
        """Test error handling when database query fails"""
        # Connect to database
        if not database.is_connected:
            await database.connect()
        
        # Temporarily disconnect database to simulate error
        await database.disconnect()
        
        with pytest.raises(Exception) as exc_info:
            await get_average_speed_by_link_day_period(1148855686, 1, 1)
        
        assert "Failed to calculate average speed aggregation for link" in str(exc_info.value)


class Test_get_link_in_box_day_period:
    """Test cases for get_link_in_box_day_period function using live PostgreSQL connection"""
    
    @pytest.mark.asyncio
    async def test_get_link_in_box_valid_bbox(self):
        """Test getting link IDs within a valid geographic bounding box"""
        # Connect to database
        if not database.is_connected:
            await database.connect()
        
        # Use bounding box around Jacksonville area (approximate)
        west = -81.8
        south = 30.1
        east = -81.6
        north = 30.3
        day = 2  # Monday
        period = 4
        
        result = await get_link_in_box_day_period(west, south, east, north, day, period)
        await database.disconnect()
        
        # Assertions
        assert isinstance(result, list)
        # Should return a list of integers (link IDs)
        for link_id in result:
            assert isinstance(link_id, int)
            assert link_id > 0
    
    @pytest.mark.asyncio
    async def test_get_link_in_box_empty_result(self):
        """Test getting link IDs with bounding box that has no data"""
        # Connect to database
        if not database.is_connected:
            await database.connect()
        
        # Use bounding box in an area with no data (middle of ocean)
        west = -90.0
        south = 25.0
        east = -89.0
        north = 26.0
        day = 2
        period = 4
        
        result = await get_link_in_box_day_period(west, south, east, north, day, period)
        await database.disconnect()
        
        # Should return empty list for area with no data
        assert isinstance(result, list)
        assert len(result) == 0
    
    @pytest.mark.asyncio
    async def test_get_link_in_box_invalid_day(self):
        """Test error handling with invalid day parameter"""
        # Connect to database
        if not database.is_connected:
            await database.connect()
        
        west = -81.8
        south = 30.1
        east = -81.6
        north = 30.3
        period = 4
        
        # Test invalid day (day < 1)
        with pytest.raises(ValueError) as exc_info:
            await get_link_in_box_day_period(west, south, east, north, 0, period)
        assert "Day must be between 1 and 7" in str(exc_info.value)
        
        # Test invalid day (day > 7)
        with pytest.raises(ValueError) as exc_info:
            await get_link_in_box_day_period(west, south, east, north, 8, period)
        assert "Day must be between 1 and 7" in str(exc_info.value)
        
        await database.disconnect()
    
    @pytest.mark.asyncio
    async def test_get_link_in_box_different_periods(self):
        """Test getting link IDs for different time periods"""
        # Connect to database
        if not database.is_connected:
            await database.connect()
        
        # Use Jacksonville area bounding box
        west = -81.8
        south = 30.1
        east = -81.6
        north = 30.3
        day = 2  # Tuesday
        periods = [1, 2, 3, 4, 5]
        
        results = []
        for period in periods:
            result = await get_link_in_box_day_period(west, south, east, north, day, period)
            results.append(result)
            
            # Basic assertions for each result
            assert isinstance(result, list)
            for link_id in result:
                assert isinstance(link_id, int)
                assert link_id > 0
        
        # Check that we got results for all periods
        assert len(results) == len(periods)
        
        await database.disconnect()
    
    @pytest.mark.asyncio
    async def test_get_link_in_box_all_weekdays(self):
        """Test getting link IDs for all weekdays"""
        # Connect to database
        if not database.is_connected:
            await database.connect()
        
        # Use Jacksonville area bounding box
        west = -81.8
        south = 30.1
        east = -81.6
        north = 30.3
        period = 4
        weekdays = [1, 2, 3, 4, 5, 6, 7]  # Monday through Sunday
        
        results = []
        for day in weekdays:
            result = await get_link_in_box_day_period(west, south, east, north, day, period)
            results.append(result)
            
            # Basic assertions
            assert isinstance(result, list)
            for link_id in result:
                assert isinstance(link_id, int)
                assert link_id > 0
        
        # Verify we got results for all weekdays
        assert len(results) == len(weekdays)
        
        await database.disconnect()
    
    @pytest.mark.asyncio
    async def test_get_link_in_box_small_vs_large_bbox(self):
        """Test that larger bounding box returns more or equal links than smaller one"""
        # Connect to database
        if not database.is_connected:
            await database.connect()
        
        day = 2
        period = 4
        
        # Small bounding box
        small_result = await get_link_in_box_day_period(-81.75, 30.15, -81.65, 30.25, day, period)
        
        # Larger bounding box (encompasses the small one)
        large_result = await get_link_in_box_day_period(-81.8, 30.1, -81.6, 30.3, day, period)
        
        # Larger bounding box should have more or equal number of links
        assert len(large_result) >= len(small_result)
        
        # All links from small bbox should be in large bbox
        for link_id in small_result:
            assert link_id in large_result
        
        await database.disconnect()
    
    @pytest.mark.asyncio
    async def test_get_link_in_box_boundary_values(self):
        """Test boundary values for coordinates and parameters"""
        # Connect to database
        if not database.is_connected:
            await database.connect()
        
        # Test with extreme coordinate values (should not cause errors)
        west = -180.0
        south = -90.0
        east = 180.0
        north = 90.0
        
        # Test minimum day and period values
        result_min = await get_link_in_box_day_period(west, south, east, north, 1, 1)
        assert isinstance(result_min, list)
        
        # Test maximum day value
        result_max_day = await get_link_in_box_day_period(west, south, east, north, 7, 1)
        assert isinstance(result_max_day, list)
        
        await database.disconnect()
    
    @pytest.mark.asyncio
    async def test_get_link_in_box_data_consistency(self):
        """Test that repeated calls with same parameters return identical results"""
        # Connect to database
        if not database.is_connected:
            await database.connect()
        
        west = -81.8
        south = 30.1
        east = -81.6
        north = 30.3
        day = 2
        period = 4
        
        # Make multiple calls
        result1 = await get_link_in_box_day_period(west, south, east, north, day, period)
        result2 = await get_link_in_box_day_period(west, south, east, north, day, period)
        result3 = await get_link_in_box_day_period(west, south, east, north, day, period)
        
        # Results should be identical
        assert result1 == result2 == result3
        
        await database.disconnect()
    
    @pytest.mark.asyncio
    async def test_get_link_in_box_distinct_results(self):
        """Test that results contain distinct link IDs (no duplicates)"""
        # Connect to database
        if not database.is_connected:
            await database.connect()
        
        west = -81.8
        south = 30.1
        east = -81.6
        north = 30.3
        day = 2
        period = 4
        
        result = await get_link_in_box_day_period(west, south, east, north, day, period)
        
        # Check that all link IDs are distinct
        assert len(result) == len(set(result)), "Result should contain distinct link IDs only"
        
        await database.disconnect()
    
    @pytest.mark.asyncio
    async def test_get_link_in_box_error_handling(self):
        """Test error handling when database query fails"""
        # Connect to database
        if not database.is_connected:
            await database.connect()
        
        # Temporarily disconnect database to simulate error
        await database.disconnect()
        
        with pytest.raises(Exception) as exc_info:
            await get_link_in_box_day_period(-81.8, 30.1, -81.6, 30.3, 2, 4)
        
        assert "Failed to retrieve links in box by day and period" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_get_link_in_box_coordinate_order(self):
        """Test different coordinate orderings (west/east, south/north)"""
        # Connect to database
        if not database.is_connected:
            await database.connect()
        
        day = 2
        period = 4
        
        # Normal order
        result_normal = await get_link_in_box_day_period(-81.8, 30.1, -81.6, 30.3, day, period)
        
        # Swapped west/east (should still work as PostGIS handles this)
        result_swapped_lon = await get_link_in_box_day_period(-81.6, 30.1, -81.8, 30.3, day, period)
        
        # Both should return the same results (PostGIS normalizes the envelope)
        assert set(result_normal) == set(result_swapped_lon)
        
        await database.disconnect()


class Test_get_slow_links_period_threshold_min_days:
    """Test cases for get_slow_links_period_threshold_min_days using live PostgreSQL connection"""

    # @pytest.fixture(autouse=True)
    async def setup_and_teardown(self):
        """Setup and teardown fixture for each test method
        Does not really work for async tests, but kept for structure.
        """
        # Setup: Connect to database
        if not database.is_connected:
            await database.connect()

        yield  # This is where the test runs

        # Teardown: Keep connection for other tests (optional cleanup)
        # We'll keep the connection alive to avoid reconnection overhead

    @pytest.mark.asyncio
    async def test_get_slow_links_period_threshold_min_days_valid_data(self):
        """Test getting average speed with valid day and period that should have data"""
        # Use period=4, threshold 20.5 and min_days=7

        # connect to the database
        if not database.is_connected:
            await database.connect()

        period = 4
        threshold = 20.5
        min_days = 7


        result = await get_slow_links_period_threshold_min_days(period, threshold, min_days)
        await database.disconnect()  # Disconnect after test

        # Assertions
        assert isinstance(result, list)

        # If data exists, check structure
        if len(result) > 0:
            for link in result:
                assert isinstance(link, dict)
                assert "link_id" in link
                assert "geometry" in link
                assert "road_name" in link
                assert "overall_average_speed" in link

                # Validate data types
                assert isinstance(link["link_id"], int)
                assert link["link_id"] > 0
                assert isinstance(link["overall_average_speed"], float)
                assert link["overall_average_speed"] >= 0.0
                assert link["overall_average_speed"] <= threshold

                # Geometry can be None or string
                if link["geometry"] is not None:
                    assert isinstance(link["geometry"], str)
                    assert link["geometry"].startswith("LINESTRING")

                # Road name can be None or string
                if link["road_name"] is not None:
                    assert isinstance(link["road_name"], str)




class Test_get_links_geometry_roadname_speed_by_day_period:
    """Test cases for get_links_geometry_roadname_speed_by_day_period function using live PostgreSQL connection"""
    
    @pytest.mark.asyncio
    async def test_get_links_valid_day_and_period(self):
        """Test getting links with valid day and period that should have data"""
        # Connect to database
        if not database.is_connected:
            await database.connect()
        
        day = 2  # Tuesday
        period = 4  # Should have data
        
        result = await get_links_geometry_roadname_speed_by_day_period(day, period)
        await database.disconnect()
        
        # Assertions
        assert isinstance(result, list)
        # If data exists, check structure
        if len(result) > 0:
            for link in result:
                assert isinstance(link, dict)
                assert "link_id" in link
                assert "geometry" in link
                assert "road_name" in link
                assert "average_speed" in link
                
                # Validate data types
                assert isinstance(link["link_id"], int)
                assert link["link_id"] > 0
                assert isinstance(link["average_speed"], float)
                assert link["average_speed"] >= 0.0
                
                # Geometry can be None or string
                if link["geometry"] is not None:
                    assert isinstance(link["geometry"], str)
                    assert link["geometry"].startswith("LINESTRING")
                
                # Road name can be None or string
                if link["road_name"] is not None:
                    assert isinstance(link["road_name"], str)
    
    @pytest.mark.asyncio
    async def test_get_links_no_data_scenario(self):
        """Test getting links with day and period that have no data"""
        # Connect to database
        if not database.is_connected:
            await database.connect()
        
        # Use parameters that are unlikely to have data
        day = 7  # Sunday
        period = 999  # Non-existent period
        
        result = await get_links_geometry_roadname_speed_by_day_period(day, period)
        await database.disconnect()
        
        # Should return empty list for no data case
        assert isinstance(result, list)
        assert len(result) == 0
    
    @pytest.mark.asyncio
    async def test_get_links_invalid_day_validation(self):
        """Test error handling with invalid day parameter"""
        # Connect to database
        if not database.is_connected:
            await database.connect()
        
        period = 4
        
        # Test invalid day (day < 1)
        with pytest.raises(ValueError) as exc_info:
            await get_links_geometry_roadname_speed_by_day_period(0, period)
        assert "Day must be between 1 and 7" in str(exc_info.value)
        
        # Test invalid day (day > 7)
        with pytest.raises(ValueError) as exc_info:
            await get_links_geometry_roadname_speed_by_day_period(8, period)
        assert "Day must be between 1 and 7" in str(exc_info.value)
        
        await database.disconnect()
    
    @pytest.mark.asyncio
    async def test_get_links_multiple_days(self):
        """Test getting links for multiple days to ensure data consistency"""
        # Connect to database
        if not database.is_connected:
            await database.connect()
        
        period = 4  # Use a period that should have data
        weekdays = [1, 2, 3, 4, 5]  # Monday through Friday
        
        results = []
        for day in weekdays:
            result = await get_links_geometry_roadname_speed_by_day_period(day, period)
            results.append(result)
            
            # Basic assertions for each result
            assert isinstance(result, list)
            
            # If data exists, validate structure
            for link in result:
                assert "link_id" in link
                assert "geometry" in link
                assert "road_name" in link
                assert "average_speed" in link
                assert isinstance(link["link_id"], int)
                assert isinstance(link["average_speed"], float)
        
        # Check that we got results for all weekdays
        assert len(results) == len(weekdays)
        
        await database.disconnect()
    
    @pytest.mark.asyncio
    async def test_get_links_multiple_periods(self):
        """Test getting links for multiple periods to ensure data consistency"""
        # Connect to database
        if not database.is_connected:
            await database.connect()
        
        day = 2  # Tuesday
        periods = [1, 2, 3, 4, 5]  # Different time periods
        
        results = []
        for period in periods:
            result = await get_links_geometry_roadname_speed_by_day_period(day, period)
            results.append(result)
            
            # Basic assertions for each result
            assert isinstance(result, list)
            
            # If data exists, validate structure
            for link in result:
                assert "link_id" in link
                assert "geometry" in link
                assert "road_name" in link
                assert "average_speed" in link
                assert isinstance(link["link_id"], int)
                assert isinstance(link["average_speed"], float)
        
        # Check that we got results for all periods
        assert len(results) == len(periods)
        
        await database.disconnect()
    
    @pytest.mark.asyncio
    async def test_get_links_speed_precision(self):
        """Test that average speed is rounded to 2 decimal places"""
        # Connect to database
        if not database.is_connected:
            await database.connect()
        
        day = 2
        period = 4
        
        result = await get_links_geometry_roadname_speed_by_day_period(day, period)
        await database.disconnect()
        
        # Check precision for all links with data
        for link in result:
            if link["average_speed"] > 0:
                speed_str = str(link["average_speed"])
                if '.' in speed_str:
                    decimal_places = len(speed_str.split('.')[1])
                    assert decimal_places <= 2, f"Average speed should be rounded to 2 decimal places, got {decimal_places}"
    
    @pytest.mark.asyncio
    async def test_get_links_geometry_format(self):
        """Test that geometry field contains valid WKT LINESTRING format"""
        # Connect to database
        if not database.is_connected:
            await database.connect()
        
        day = 2
        period = 4
        
        result = await get_links_geometry_roadname_speed_by_day_period(day, period)
        await database.disconnect()
        
        # Check geometry format for all links with geometry data
        for link in result:
            if link["geometry"] is not None:
                geometry = link["geometry"]
                assert isinstance(geometry, str)
                assert geometry.startswith("LINESTRING")
                assert "(" in geometry and ")" in geometry
                # Basic validation that it contains coordinate pairs
                coords_part = geometry[geometry.find("(")+1:geometry.rfind(")")]
                assert len(coords_part.strip()) > 0
    
    @pytest.mark.asyncio
    async def test_get_links_distinct_link_ids(self):
        """Test that results contain distinct link IDs (no duplicates)"""
        # Connect to database
        if not database.is_connected:
            await database.connect()
        
        day = 2
        period = 4
        
        result = await get_links_geometry_roadname_speed_by_day_period(day, period)
        await database.disconnect()
        
        # Extract all link IDs
        link_ids = [link["link_id"] for link in result]
        
        # Check that all link IDs are distinct
        assert len(link_ids) == len(set(link_ids)), "Result should contain distinct link IDs only"
    
    @pytest.mark.asyncio
    async def test_get_links_boundary_values(self):
        """Test boundary values for day and period parameters"""
        # Connect to database
        if not database.is_connected:
            await database.connect()
        
        # Test minimum values
        result_min = await get_links_geometry_roadname_speed_by_day_period(1, 1)
        assert isinstance(result_min, list)
        
        # Test maximum day value
        result_max_day = await get_links_geometry_roadname_speed_by_day_period(7, 1)
        assert isinstance(result_max_day, list)
        
        await database.disconnect()
    
    @pytest.mark.asyncio
    async def test_get_links_data_consistency(self):
        """Test that repeated calls with same parameters return identical results"""
        # Connect to database
        if not database.is_connected:
            await database.connect()
        
        day = 2
        period = 4
        
        # Make multiple calls
        result1 = await get_links_geometry_roadname_speed_by_day_period(day, period)
        result2 = await get_links_geometry_roadname_speed_by_day_period(day, period)
        result3 = await get_links_geometry_roadname_speed_by_day_period(day, period)
        
        # Results should be identical
        assert result1 == result2 == result3
        
        await database.disconnect()
    
    @pytest.mark.asyncio
    async def test_get_links_ordered_by_link_id(self):
        """Test that results are ordered by link_id"""
        # Connect to database
        if not database.is_connected:
            await database.connect()
        
        day = 2
        period = 4
        
        result = await get_links_geometry_roadname_speed_by_day_period(day, period)
        await database.disconnect()
        
        # Check that link IDs are in ascending order
        if len(result) > 1:
            link_ids = [link["link_id"] for link in result]
            assert link_ids == sorted(link_ids), "Results should be ordered by link_id"
    
    @pytest.mark.asyncio
    async def test_get_links_error_handling_database_failure(self):
        """Test error handling when database query fails"""
        # Connect to database
        if not database.is_connected:
            await database.connect()
        
        # Temporarily disconnect database to simulate error
        await database.disconnect()
        
        with pytest.raises(Exception) as exc_info:
            await get_links_geometry_roadname_speed_by_day_period(2, 4)
        
        assert "Failed to get simplified aggregate data" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_get_links_fields_structure(self):
        """Test that all expected fields are present in each link"""
        # Connect to database
        if not database.is_connected:
            await database.connect()
        
        day = 2
        period = 4
        
        result = await get_links_geometry_roadname_speed_by_day_period(day, period)
        await database.disconnect()
        
        # Check that all expected fields are present
        expected_fields = ["link_id", "geometry", "road_name", "average_speed"]
        
        for link in result:
            for field in expected_fields:
                assert field in link, f"Field '{field}' missing from link data"
    
    @pytest.mark.asyncio
    async def test_get_links_average_speed_aggregation(self):
        """Test that average speeds are properly aggregated across multiple records"""
        # Connect to database
        if not database.is_connected:
            await database.connect()
        
        day = 2
        period = 4
        
        result = await get_links_geometry_roadname_speed_by_day_period(day, period)
        await database.disconnect()
        
        # Verify that average speeds are reasonable values
        for link in result:
            speed = link["average_speed"]
            assert isinstance(speed, float)
            assert speed >= 0.0
            assert speed <= 200.0  # Reasonable upper bound for speed in mph/kmh
    
    @pytest.mark.asyncio
    async def test_get_links_compare_with_full_aggregate(self):
        """Test that simplified function returns subset of data from full aggregate function"""
        # Connect to database
        if not database.is_connected:
            await database.connect()
        
        day = 2
        period = 4
        
        # Get data from both functions
        simplified_result = await get_links_geometry_roadname_speed_by_day_period(day, period)
        full_result = await get_average_speed_by_day_period(day, period)
        
        await database.disconnect()
        
        # If both have data, verify consistency
        if len(simplified_result) > 0 and len(full_result.get("links", [])) > 0:
            simplified_link_ids = {link["link_id"] for link in simplified_result}
            full_link_ids = {link["link_id"] for link in full_result["links"]}
            
            # Simplified should be a subset of full results
            assert simplified_link_ids.issubset(full_link_ids) or simplified_link_ids == full_link_ids

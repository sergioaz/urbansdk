import pytest
from decimal import Decimal
from app.services.aggregate import get_average_speed_by_day_period, get_average_speed_by_link_day_period
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
        
        # Assertions
        assert isinstance(result, dict)
        assert "link_id" in result
        assert "day_of_week" in result
        assert "period" in result
        assert "average_speed" in result
        assert "road_name" in result
        assert "length" in result
        
        assert result["link_id"] == link_id
        assert result["day_of_week"] == day
        assert result["period"] == period
        assert isinstance(result["average_speed"], float)
        assert result["average_speed"] >= 0.0
        
        # Check metadata fields
        assert "usdk_speed_category" in result
        assert "funclass_id" in result
        assert "volume_value" in result
        assert "geo_json" in result
    
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
        
        # Check all expected fields are present
        expected_fields = [
            "link_id", "day_of_week", "period", "average_speed", "average_freeflow",
            "record_count", "length", "road_name", "usdk_speed_category",
            "funclass_id", "speedcat", "volume_value", "volume_bin_id",
            "volume_year", "volumes_bin_description", "geo_json"
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


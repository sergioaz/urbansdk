import pytest
from decimal import Decimal
from app.services.aggregate import get_average_speed_by_day_period
from app.db.database import database


class TestAggregateService:
    """Test cases for aggregate service functions using live PostgreSQL connection"""

    @pytest.mark.asyncio
    async def test_get_average_speed_valid_data(self):
        """Test getting average speed with valid day and period that should have data"""
        # Use day=2 (Tuesday) and period=4 which should have data based on your sample
        day = 2
        period = 4
        
        result = await get_average_speed_by_day_period(day, period)
        
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
        day = 7
        period = 999
        
        result = await get_average_speed_by_day_period(day, period)
        
        # Assertions for no data case
        assert isinstance(result, dict)
        assert result["day_of_week"] == day
        assert result["period"] == period
        assert result["average_speed"] == 0.0

    @pytest.mark.asyncio
    async def test_get_average_speed_monday_period_1(self):
        """Test getting average speed for Monday period 1"""
        day = 1  # Monday
        period = 1
        
        result = await get_average_speed_by_day_period(day, period)
        
        assert result["day_of_week"] == 1
        assert result["period"] == 1
        assert isinstance(result["average_speed"], float)

    @pytest.mark.asyncio
    async def test_get_average_speed_multiple_periods(self):
        """Test getting average speed for multiple periods to ensure data consistency"""
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

    @pytest.mark.asyncio
    async def test_get_average_speed_all_weekdays(self):
        """Test getting average speed for all weekdays with a common period"""
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

    @pytest.mark.asyncio
    async def test_average_speed_precision(self):
        """Test that average speed is rounded to 2 decimal places"""
        day = 2
        period = 4
        
        result = await get_average_speed_by_day_period(day, period)
        
        # Check precision (should be rounded to 2 decimal places)
        speed_str = str(result["average_speed"])
        if '.' in speed_str:
            decimal_places = len(speed_str.split('.')[1])
            assert decimal_places <= 2, f"Average speed should be rounded to 2 decimal places, got {decimal_places}"

    @pytest.mark.asyncio
    async def test_error_handling_invalid_database(self):
        """Test error handling when database query fails"""
        # Temporarily disconnect database to simulate error
        await database.disconnect()
        
        with pytest.raises(Exception) as exc_info:
            await get_average_speed_by_day_period(1, 1)
        
        assert "Failed to calculate average speed aggregation" in str(exc_info.value)
        
        # Reconnect for other tests
        await database.connect()

    @pytest.mark.asyncio
    async def test_edge_case_boundary_values(self):
        """Test boundary values for day and period"""
        # Test minimum values
        result_min = await get_average_speed_by_day_period(1, 1)
        assert result_min["day_of_week"] == 1
        assert result_min["period"] == 1
        
        # Test maximum day value
        result_max_day = await get_average_speed_by_day_period(7, 1)
        assert result_max_day["day_of_week"] == 7
        assert result_max_day["period"] == 1

    @pytest.mark.asyncio 
    async def test_data_consistency(self):
        """Test that repeated calls return consistent results"""
        day = 2
        period = 4
        
        # Make multiple calls
        result1 = await get_average_speed_by_day_period(day, period)
        result2 = await get_average_speed_by_day_period(day, period)
        result3 = await get_average_speed_by_day_period(day, period)
        
        # Results should be identical
        assert result1 == result2 == result3

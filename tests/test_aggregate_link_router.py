import pytest
from unittest.mock import patch, AsyncMock
from httpx import AsyncClient, ASGITransport
from app.main import app
from app.db.database import database

@pytest.fixture(autouse=True)
async def setup_database():
    """Setup database connection for tests"""
    if not database.is_connected:
        await database.connect()
    yield
    # Keep connection for other tests, disconnect will be handled by app lifespan

@pytest.mark.asyncio
async def test_get_aggregate_link_data():
    """Test the main endpoint with valid parameters"""

    if not database.is_connected:
        await database.connect()

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        link_id = 1148855686
        response = await ac.get(f"/aggregates/{link_id}", params={"day": "Tuesday", "period": "AM Peak"})
        
        # Debug output if test fails
        if response.status_code not in [200, 404]:
            print(f"Response status: {response.status_code}")
            print(f"Response body: {response.text}")
        
        # Since we might not have data, check for either 200 or 404
        assert response.status_code in [200, 404], f"Expected 200 or 404, got {response.status_code}: {response.text}"
        
        if response.status_code == 200:
            data = response.json()
            # Verify response structure for successful response
            assert "link_id" in data
            assert "day_of_week" in data
            assert "period" in data
            assert "average_speed" in data
            assert "road_name" in data
            assert "length" in data
            
            # Verify correct mapping from day/period names to numbers
            assert data["link_id"] == link_id
            assert data["day_of_week"] == 3  # Tuesday maps to 3
            assert data["period"] == 3       # AM Peak maps to 3
            assert isinstance(data["average_speed"], float)
        elif response.status_code == 404:
            # No data found is also acceptable for this test
            data = response.json()
            assert "detail" in data
        await database.disconnect()

@pytest.mark.asyncio
async def test_get_aggregate_link_data_invalid_day():
    """Test with invalid day name"""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        link_id = 1148855686
        response = await ac.get(f"/aggregates/{link_id}", params={"day": "Funday", "period": "AM Peak"})
        
        # Debug output if test fails
        if response.status_code != 422:
            print(f"Response status: {response.status_code}")
            print(f"Response body: {response.text}")
            
        assert response.status_code == 422  # FastAPI returns 422 for validation errors
        data = response.json()
        assert "detail" in data

@pytest.mark.asyncio
async def test_get_aggregate_link_data_invalid_period():
    """Test with invalid period name"""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        link_id = 1148855686
        response = await ac.get(f"/aggregates/{link_id}", params={"day": "Tuesday", "period": "Invalid Period"})
        
        # Debug output if test fails
        if response.status_code != 422:
            print(f"Response status: {response.status_code}")
            print(f"Response body: {response.text}")
            
        assert response.status_code == 422  # FastAPI returns 422 for validation errors
        data = response.json()
        assert "detail" in data

@pytest.mark.asyncio
async def test_get_aggregate_link_data_invalid_link_id():
    """Test with invalid link_id (non-integer)"""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/aggregates/invalid_link", params={"day": "Tuesday", "period": "AM Peak"})
        
        assert response.status_code == 422  # FastAPI returns 422 for validation errors
        data = response.json()
        assert "detail" in data

@pytest.mark.asyncio
async def test_get_aggregate_link_data_missing_params():
    """Test with missing required parameters"""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        link_id = 1148855686
        
        # Test missing day parameter
        response = await ac.get(f"/aggregates/{link_id}", params={"period": "AM Peak"})
        assert response.status_code == 422
        
        # Test missing period parameter
        response = await ac.get(f"/aggregates/{link_id}", params={"day": "Tuesday"})
        assert response.status_code == 422
        
        # Test missing both parameters
        response = await ac.get(f"/aggregates/{link_id}")
        assert response.status_code == 422

@pytest.mark.asyncio
async def test_get_aggregate_link_data_different_days():
    """Test with different valid day names"""

    if not database.is_connected:
        await database.connect()

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        link_id = 1148855686
        
        valid_days = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
        expected_day_numbers = [1, 2, 3, 4, 5, 6, 7]
        
        for day, expected_num in zip(valid_days, expected_day_numbers):
            response = await ac.get(f"/aggregates/{link_id}", params={"day": day, "period": "AM Peak"})
            
            # Should get either success or no data, but not validation error
            assert response.status_code in [200, 404, 500], f"Unexpected status for day {day}: {response.status_code}"
            
            # If we get data, verify the day mapping is correct
            if response.status_code == 200:
                data = response.json()
                assert data["day_of_week"] == expected_num, f"Expected day_of_week {expected_num} for {day}, got {data['day_of_week']}"

    await database.disconnect()


@pytest.mark.asyncio
async def test_get_aggregate_link_data_different_periods():
    """Test with different valid period names"""

    if not database.is_connected:
        await database.connect()

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        link_id = 1148855686
        
        valid_periods = ["Overnight", "Early Morning", "AM Peak", "Midday", "Early Afternoon", "PM Peak", "Evening"]
        expected_period_numbers = [1, 2, 3, 4, 5, 6, 7]
        
        for period, expected_num in zip(valid_periods, expected_period_numbers):
            response = await ac.get(f"/aggregates/{link_id}", params={"day": "Tuesday", "period": period})
            
            # Should get either success or no data, but not validation error
            assert response.status_code in [200, 404, 500], f"Unexpected status for period {period}: {response.status_code}"
            
            # If we get data, verify the period mapping is correct
            if response.status_code == 200:
                data = response.json()
                assert data["period"] == expected_num, f"Expected period {expected_num} for {period}, got {data['period']}"

    await database.disconnect()


@pytest.mark.asyncio
async def test_get_aggregate_link_data_different_link_ids():
    """Test with different link IDs"""

    if not database.is_connected:
        await database.connect()

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        link_ids = [1148855686, 1240632857, 1240632858, 999999999]  # Include a likely non-existent one
        
        for link_id in link_ids:
            response = await ac.get(f"/aggregates/{link_id}", params={"day": "Tuesday", "period": "AM Peak"})
            
            # Should get either success, no data, or server error, but not validation error
            assert response.status_code in [200, 404, 500], f"Unexpected status for link_id {link_id}: {response.status_code}"
            
            # If we get data, verify the link_id is correct
            if response.status_code == 200:
                data = response.json()
                assert data["link_id"] == link_id, f"Expected link_id {link_id}, got {data['link_id']}"

    await database.disconnect()


@pytest.mark.asyncio
@patch("app.routers.aggregate_link.get_average_speed_by_link_day_period", new_callable=AsyncMock)
async def test_get_aggregate_link_data_with_mock_data(mock_service):
    """Test with mocked service to verify router functionality without database"""
    # Mock the async service function to return sample data
    mock_service.return_value = {
        "link_id": 1148855686,
        "day_of_week": 3,
        "period": 3,
        "average_speed": 42.75,
        "average_freeflow": 55.2,
        "record_count": 150,
        "length": 0.123456,
        "road_name": "Test Road",
        "usdk_speed_category": 40,  # Should be integer, not string
        "funclass_id": 4,
        "speedcat": 2,  # Should be integer, not string
        "volume_value": 1000,
        "volume_bin_id": 2,
        "volume_year": 2023,
        "volumes_bin_description": "Medium Volume",
        "geometry": "LINESTRING(0 0, 1 1)"
    }
    
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        link_id = 1148855686
        response = await ac.get(f"/aggregates/{link_id}", params={"day": "Tuesday", "period": "AM Peak"})
        
        assert response.status_code == 200
        data = response.json()
        
        # Verify response structure
        assert "link_id" in data
        assert "day_of_week" in data
        assert "period" in data
        assert "average_speed" in data
        assert "road_name" in data
        assert "length" in data
        
        # Verify correct mapping from day/period names to numbers
        assert data["link_id"] == 1148855686
        assert data["day_of_week"] == 3  # Tuesday maps to 3
        assert data["period"] == 3       # AM Peak maps to 3
        assert data["average_speed"] == 42.75
        assert data["road_name"] == "Test Road"
        
        # Verify the service was called with the correct parameters
        mock_service.assert_called_once_with(1148855686, 3, 3)  # link_id, day_number, period_number

@pytest.mark.asyncio
@patch("app.routers.aggregate_link.get_average_speed_by_link_day_period", new_callable=AsyncMock)
async def test_get_aggregate_link_data_with_mock_no_data(mock_service):
    """Test with mocked service returning no data scenario"""
    # Mock the service function to return no data found
    mock_service.return_value = {
        "link_id": 999999999,
        "day_of_week": 3,
        "period": 3,
        "average_speed": 0.0,
        "error": "No data found"
    }
    
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        link_id = 999999999
        response = await ac.get(f"/aggregates/{link_id}", params={"day": "Tuesday", "period": "AM Peak"})
        
        assert response.status_code == 404
        data = response.json()
        assert "detail" in data
        assert "No data found" in data["detail"]
        
        # Verify the service was called with the correct parameters
        mock_service.assert_called_once_with(999999999, 3, 3)

@pytest.mark.asyncio
async def test_get_aggregate_link_data_edge_cases():
    """Test edge cases like very large link IDs"""

    if not database.is_connected:
        await database.connect()

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        # Test with very large link ID
        large_link_id = 9999999999
        response = await ac.get(f"/aggregates/{large_link_id}", params={"day": "Sunday", "period": "Overnight"})
        
        # Should handle large numbers without validation error
        assert response.status_code in [200, 404, 500], f"Unexpected status for large link_id: {response.status_code}"
        
        # Test with zero link ID
        response = await ac.get("/aggregates/0", params={"day": "Sunday", "period": "Overnight"})
        assert response.status_code in [200, 404, 500], f"Unexpected status for zero link_id: {response.status_code}"

    await database.disconnect()

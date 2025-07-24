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
async def test_get_aggregated_speed():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/aggregates/", params={"day": "Tuesday", "period": "AM Peak"})
        
        # Debug output if test fails
        if response.status_code != 200:
            print(f"Response status: {response.status_code}")
            print(f"Response body: {response.text}")
        
        # Since we might not have data, check for either 200 or 404
        assert response.status_code in [200, 404], f"Expected 200 or 404, got {response.status_code}: {response.text}"
        
        if response.status_code == 200:
            data = response.json()
            assert "day_of_week" in data
            assert "period" in data
            assert "average_speed" in data
            assert data["day_of_week"] == 3  # Numerical mapping for Tuesday
            assert data["period"] == 3       # Numerical mapping for AM Peak
            assert isinstance(data["average_speed"], float)
        elif response.status_code == 404:
            # No data found is also acceptable for this test
            data = response.json()
            assert "detail" in data

@pytest.mark.asyncio
async def test_get_aggregated_speed_invalid_day():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/aggregates/", params={"day": "Funday", "period": "AM Peak"})
        
        # Debug output if test fails
        if response.status_code != 422:
            print(f"Response status: {response.status_code}")
            print(f"Response body: {response.text}")
            
        assert response.status_code == 422  # FastAPI returns 422 for validation errors
        data = response.json()
        assert "detail" in data

@pytest.mark.asyncio
async def test_get_aggregated_speed_invalid_period():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/aggregates/", params={"day": "Tuesday", "period": "Invalid Period"})
        
        # Debug output if test fails
        if response.status_code != 422:
            print(f"Response status: {response.status_code}")
            print(f"Response body: {response.text}")
            
        assert response.status_code == 422  # FastAPI returns 422 for validation errors
        data = response.json()
        assert "detail" in data

@pytest.mark.asyncio
async def test_get_aggregated_speed_missing_params():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        # Test missing day parameter
        response = await ac.get("/aggregates/", params={"period": "AM Peak"})
        assert response.status_code == 422
        
        # Test missing period parameter
        response = await ac.get("/aggregates/", params={"day": "Tuesday"})
        assert response.status_code == 422
        
        # Test missing both parameters
        response = await ac.get("/aggregates/")
        assert response.status_code == 422

@pytest.mark.asyncio
@patch("app.services.aggregate.get_average_speed_by_day_period", new_callable=AsyncMock)
async def test_get_aggregated_speed_with_mock_data(mock_service):
    """Test with mocked service to verify router functionality without database"""
    # Mock the async service function to return sample data
    mock_service.return_value = {
        "day_of_week": 3,
        "period": 3,
        "average_speed": 42.75
    }
    
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/aggregates/", params={"day": "Tuesday", "period": "AM Peak"})
        
        assert response.status_code == 200
        data = response.json()
        
        # Verify response structure
        assert "day_of_week" in data
        assert "period" in data
        assert "average_speed" in data
        
        # Verify correct mapping from day/period names to numbers
        assert data["day_of_week"] == 3  # Tuesday maps to 3
        assert data["period"] == 3       # AM Peak maps to 3  
        assert data["average_speed"] == 42.75
        
        # Verify the service was called with the correct numeric parameters
        mock_service.assert_called_once_with(3, 3)  # day_number=3, period_number=3


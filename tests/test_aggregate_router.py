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
    # Ensure database is connected
    if not database.is_connected:
        await database.connect()
    
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/aggregates/", params={"day": "Tuesday", "period": "AM Peak"})
        
        # Debug output if test fails
        if response.status_code not in [200, 404]:
            print(f"Response status: {response.status_code}")
            print(f"Response body: {response.text}")
        
        # Since we might not have data, check for either 200 or 404
        assert response.status_code in [200, 404], f"Expected 200 or 404, got {response.status_code}: {response.text}"
        
        if response.status_code == 200:
            data = response.json()
            # Verify response is a list of links
            assert isinstance(data, list)
            
            # If there are links, verify their structure
            for link in data:
                assert "link_id" in link
                assert "road_name" in link
                assert "geometry" in link
                assert "average_speed" in link
                assert isinstance(link["link_id"], int)
                assert isinstance(link["average_speed"], (int, float))
        elif response.status_code == 404:
            # No data found is also acceptable for this test
            data = response.json()
            assert "detail" in data
    
    await database.disconnect()

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
@patch("app.routers.aggregate.get_links_geometry_roadname_speed_by_day_period", new_callable=AsyncMock)
async def test_get_aggregated_speed_with_mock_data(mock_service):
    """Test with mocked service to verify router functionality without database"""
    # Mock the async service function to return list of links directly
    mock_service.return_value = [
        {
            "link_id": 1148855686,
            "road_name": "Test Road",
            "geometry": "LINESTRING(-81.51023 30.16599, -81.51038 30.16637)",
            "average_speed": 42.75
        }
    ]
    
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/aggregates/", params={"day": "Tuesday", "period": "AM Peak"})
        
        assert response.status_code == 200
        data = response.json()
        
        # Verify response is a list of links
        assert isinstance(data, list)
        assert len(data) == 1
        
        # Verify link structure
        link = data[0]
        assert "link_id" in link
        assert "road_name" in link
        assert "geometry" in link
        assert "average_speed" in link
        
        # Verify link data
        assert link["link_id"] == 1148855686
        assert link["road_name"] == "Test Road"
        assert link["geometry"] == "LINESTRING(-81.51023 30.16599, -81.51038 30.16637)"
        assert link["average_speed"] == 42.75
        
        # Verify the service was called with the correct numeric parameters
        mock_service.assert_called_once_with(3, 3)  # day_number=3, period_number=3


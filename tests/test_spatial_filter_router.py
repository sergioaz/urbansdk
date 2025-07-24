import pytest
from httpx import AsyncClient, ASGITransport
from fastapi import HTTPException
from app.main import app
from app.db.database import database
import logging

class TestSpatialFilterRouter:
    """Test cases for spatial filter router"""
    
    # Removed global database fixture - using manual connection management per test
    
    @pytest.mark.asyncio
    async def test_spatial_filter_valid_request(self):
        """Test spatial filter with valid request data"""
        try:
            async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
                # Valid request with Jacksonville area bounding box
                request_data = {
                    "day": "Tuesday",
                    "period": "AM Peak",
                    "bbox": [-81.8, 30.1, -81.6, 30.3]
                }

                if not database.is_connected:
                    await database.connect()
                response = await client.post("/spatial-filter/", json=request_data)
                await database.disconnect()

                assert response.status_code == 200
                data = response.json()

                # Check response structure
                assert "link_ids" in data
                assert "count" in data
                assert "day_of_week" in data
                assert "period" in data
                assert "bbox" in data

                # Check data types
                assert isinstance(data["link_ids"], list)
                assert isinstance(data["count"], int)
                assert isinstance(data["day_of_week"], int)
                assert isinstance(data["period"], int)
                assert isinstance(data["bbox"], list)

                # Check data values
                assert data["day_of_week"] == 3  # Tuesday = 3
                assert data["period"] == 3       # AM Peak = 3
                assert data["bbox"] == [-81.8, 30.1, -81.6, 30.3]
                assert data["count"] == len(data["link_ids"])

                # Check that all link_ids are integers
                for link_id in data["link_ids"]:
                    assert isinstance(link_id, int)
                    assert link_id > 0
        except Exception as e:
            logging.error(f"Spatial filter error: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    @pytest.mark.asyncio
    async def test_spatial_filter_empty_area(self):
        """Test spatial filter with bounding box that should return no results"""
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            # Bounding box in middle of ocean (should return no links)
            request_data = {
                "day": "Monday",
                "period": "Evening", 
                "bbox": [-90.0, 25.0, -89.0, 26.0]
            }

            if not database.is_connected:
                await database.connect()
            response = await client.post("/spatial-filter/", json=request_data)

            assert response.status_code == 200
            data = response.json()
            
            assert data["link_ids"] == []
            assert data["count"] == 0
            assert data["day_of_week"] == 2  # Monday = 2
            assert data["period"] == 7       # Evening = 7

        await database.disconnect()

    @pytest.mark.asyncio
    async def test_spatial_filter_invalid_day(self):
        """Test spatial filter with invalid day name"""
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            request_data = {
                "day": "InvalidDay",
                "period": "AM Peak",
                "bbox": [-81.8, 30.1, -81.6, 30.3]
            }
            
            response = await client.post("/spatial-filter/", json=request_data)
            
            assert response.status_code == 422  # Pydantic validation error



    @pytest.mark.asyncio
    async def test_spatial_filter_invalid_period(self):
        """Test spatial filter with invalid period name"""
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            request_data = {
                "day": "Tuesday",
                "period": "InvalidPeriod",
                "bbox": [-81.8, 30.1, -81.6, 30.3]
            }
            
            response = await client.post("/spatial-filter/", json=request_data)
            
            assert response.status_code == 422  # Pydantic validation error
    
    @pytest.mark.asyncio
    async def test_spatial_filter_invalid_bbox_length(self):
        """Test spatial filter with invalid bounding box length"""
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            request_data = {
                "day": "Tuesday",
                "period": "AM Peak",
                "bbox": [-81.8, 30.1, -81.6]  # Missing north coordinate
            }
            
            response = await client.post("/spatial-filter/", json=request_data)
            
            assert response.status_code == 422  # Pydantic validation error
    
    @pytest.mark.asyncio
    async def test_spatial_filter_invalid_bbox_coordinates(self):
        """Test spatial filter with invalid coordinate values"""
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            # Invalid longitude (> 180)
            request_data = {
                "day": "Tuesday",
                "period": "AM Peak",
                "bbox": [200.0, 30.1, -81.6, 30.3]
            }
            
            response = await client.post("/spatial-filter/", json=request_data)
            
            assert response.status_code == 422  # Pydantic validation error
    
    @pytest.mark.asyncio
    async def test_spatial_filter_invalid_bbox_order(self):
        """Test spatial filter with invalid bounding box coordinate order"""
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            # West > East (invalid)
            request_data = {
                "day": "Tuesday",
                "period": "AM Peak",
                "bbox": [-81.6, 30.1, -81.8, 30.3]
            }
            
            response = await client.post("/spatial-filter/", json=request_data)
            
            assert response.status_code == 422  # Pydantic validation error
    
    @pytest.mark.asyncio
    async def test_spatial_filter_all_days(self):
        """Test spatial filter with all valid day names"""
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            days = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
            expected_day_numbers = [1, 2, 3, 4, 5, 6, 7]
            if not database.is_connected:
                await database.connect()
            for day, expected_day_num in zip(days, expected_day_numbers):
                request_data = {
                    "day": day,
                    "period": "Midday",
                    "bbox": [-81.8, 30.1, -81.6, 30.3]
                }
                
                response = await client.post("/spatial-filter/", json=request_data)


                assert response.status_code == 200
                data = response.json()
                assert data["day_of_week"] == expected_day_num
        await database.disconnect()
    
    @pytest.mark.asyncio
    async def test_spatial_filter_all_periods(self):
        """Test spatial filter with all valid period names"""
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            periods = ["Overnight", "Early Morning", "AM Peak", "Midday", "Early Afternoon", "PM Peak", "Evening"]
            expected_period_numbers = [1, 2, 3, 4, 5, 6, 7]
            if not database.is_connected:
                await database.connect()
            for period, expected_period_num in zip(periods, expected_period_numbers):
                request_data = {
                    "day": "Tuesday",
                    "period": period,
                    "bbox": [-81.8, 30.1, -81.6, 30.3]
                }
                
                response = await client.post("/spatial-filter/", json=request_data)
                
                assert response.status_code == 200
                data = response.json()
                assert data["period"] == expected_period_num

        await database.disconnect()

    
    @pytest.mark.asyncio
    async def test_spatial_filter_different_bbox_sizes(self):
        """Test spatial filter with different bounding box sizes"""
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            # Small bounding box
            small_request = {
                "day": "Tuesday",
                "period": "AM Peak",
                "bbox": [-81.75, 30.15, -81.65, 30.25]
            }
            
            # Larger bounding box (encompasses the small one)
            large_request = {
                "day": "Tuesday",
                "period": "AM Peak",
                "bbox": [-81.8, 30.1, -81.6, 30.3]
            }
            if not database.is_connected:
                await database.connect()
            small_response = await client.post("/spatial-filter/", json=small_request)
            large_response = await client.post("/spatial-filter/", json=large_request)

            await database.disconnect()

            assert small_response.status_code == 200
            assert large_response.status_code == 200
            
            small_data = small_response.json()
            large_data = large_response.json()
            
            # Larger bounding box should return more or equal links
            assert large_data["count"] >= small_data["count"]
            
            # All links from small bbox should be in large bbox
            for link_id in small_data["link_ids"]:
                assert link_id in large_data["link_ids"]
    
    @pytest.mark.asyncio
    async def test_spatial_filter_response_format(self):
        """Test that spatial filter response matches expected schema"""
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            request_data = {
                "day": "Wednesday",
                "period": "PM Peak",
                "bbox": [-81.8, 30.1, -81.6, 30.3]
            }
            if not database.is_connected:
                await database.connect()
            response = await client.post("/spatial-filter/", json=request_data)
            await database.disconnect()

            assert response.status_code == 200
            data = response.json()
            
            # Verify all required fields are present
            required_fields = ["link_ids", "count", "day_of_week", "period", "bbox"]
            for field in required_fields:
                assert field in data
            
            # Verify field types match schema
            assert isinstance(data["link_ids"], list)
            assert isinstance(data["count"], int)
            assert isinstance(data["day_of_week"], int)
            assert isinstance(data["period"], int)
            assert isinstance(data["bbox"], list)
            assert len(data["bbox"]) == 4
            
            # Verify data consistency
            assert data["count"] == len(data["link_ids"])
            assert data["day_of_week"] == 4  # Wednesday = 4
            assert data["period"] == 6       # PM Peak = 6
            assert data["bbox"] == [-81.8, 30.1, -81.6, 30.3]

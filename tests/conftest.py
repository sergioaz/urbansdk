import os
import pytest
from typing import Generator, AsyncGenerator
import asyncio
from fastapi.testclient import TestClient
from httpx import AsyncClient, ASGITransport

import databases
from app.core.config import config
from app.db.database import database, metadata, engine
from app.main import app

os.environ["ENV_STATE"] = "dev"

# runs once per session
@pytest.fixture(scope = "session")
def anyio_backend():
    return "asyncio"

@pytest.fixture()
def client() -> Generator:
    with TestClient(app) as c:
        yield c

@pytest.fixture()
async def async_client(client: TestClient)     -> AsyncGenerator:
    async with AsyncClient(transport=ASGITransport(app),
        base_url=client.base_url) as ac:
        yield ac


@pytest.fixture
def database_connection():
    return database


@pytest.fixture(scope="session", autouse=True)
async def setup_database():
    """Setup database connection for all tests."""
    await database.connect()
    yield
    await database.disconnect()



import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.exception_handlers import http_exception_handler

from app.routers.aggregate import router as aggregate_router
from app.routers.aggregate_link import router as aggregate_link_router
from app.routers.spatial_filter import router as spatial_filter_router
from app.db.database import database
from app.core.logging_conf import configure_logging

logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app : FastAPI):
    configure_logging()
    logger.info("Starting app...")
    await database.connect()
    yield
    await database.disconnect()
app = FastAPI(lifespan=lifespan)


app.include_router(aggregate_router, tags=["get_aggregated_speed"])
app.include_router(aggregate_link_router, tags=["get_aggregate_link_data"])
app.include_router(spatial_filter_router, tags=["spatial_filter"])


@app.exception_handler(HTTPException)
async def http_exception_handle_logging(request, exc):
    logger.error(f"HTTPException: {exc.status_code=}, {exc.detail=}")
    return await http_exception_handler(request, exc)
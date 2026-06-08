import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Import Database Managers
from core.db_clickhouse import ch_manager
from core.db_postgres import pg_manager
from core.redis_client import redis_manager

# Import Domain Routers
from detective.router import router as detective_router
# from signal.router import router as signal_router # We will uncomment this later

# Configure global logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("SignalForge.Main")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages the global lifecycle of the FastAPI application.
    Executes startup logic before receiving requests, and shutdown logic on exit.
    """
    logger.info("Starting up SignalForge API...")

    # --- Startup Phase: Initialize all connection pools ---
    try:
        ch_manager.connect()
        await pg_manager.connect()
        await redis_manager.connect()
        logger.info("All database connection pools established successfully.")
    except Exception as e:
        logger.critical("Failed to initialize database connections: %s", str(e))
        raise e

    yield # The application runs and handles requests here

    # --- Shutdown Phase: Gracefully close all connections ---
    logger.info("Shutting down SignalForge API...")
    try:
        await redis_manager.disconnect()
        await pg_manager.disconnect()
        ch_manager.disconnect()
        logger.info("All database connections closed gracefully.")
    except Exception as e:
        logger.error("Error during connection shutdown: %s", str(e))

# Initialize the FastAPI application
app = FastAPI(
    title="SignalForge API",
    description="Backend API for SignalForge and Address Detective",
    version="1.0.0",
    lifespan=lifespan
)

# Configure CORS for Frontend integration (Hackathon permissive mode)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # In production, restrict this to your frontend domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount Domain Routers
app.include_router(detective_router)
# app.include_router(signal_router)

@app.get("/health", tags=["System"])
async def health_check():
    """
    Basic health check endpoint for load balancers or Kubernetes probes.
    """
    return {
        "status": "healthy",
        "service": "SignalForge API",
        "timestamp": __import__("datetime").datetime.utcnow().isoformat()
    }
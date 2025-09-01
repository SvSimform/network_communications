"""
FastAPI Communication Techniques Demo
Demonstrates: Long Polling, SSE, MQTT, WebSocket, Socket.IO
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import uvicorn
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    logger.info("🚀 FastAPI Communication Demo Starting...")
    yield
    logger.info("🔥 FastAPI Communication Demo Shutting down...")

# Initialize FastAPI app
app = FastAPI(
    title="Communication Techniques Demo",
    description="Demonstrates Long Polling, SSE, MQTT, WebSocket, and Socket.IO",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mock data for demonstrations
MOCK_DATA = {
    "notifications": [
        {"id": 1, "message": "Welcome to the demo!", "timestamp": "2025-09-01T10:00:00Z"},
        {"id": 2, "message": "Long polling update available", "timestamp": "2025-09-01T10:01:00Z"},
        {"id": 3, "message": "Real-time data incoming", "timestamp": "2025-09-01T10:02:00Z"},
    ],
    "status": "active",
    "users_online": 42
}

@app.get("/")
async def root():
    """Root endpoint with API information"""
    try:
        return {
            "message": "FastAPI Communication Techniques Demo",
            "version": "1.0.0",
            "available_techniques": [
                "Long Polling",
                "Server-Sent Events (SSE)",
                "MQTT",
                "WebSocket",
                "Socket.IO"
            ],
            "status": "ready"
        }
    except Exception as e:
        logger.error(f"Error in root endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        return {
            "status": "healthy",
            "timestamp": "2025-09-01T10:00:00Z"
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        raise HTTPException(status_code=500, detail="Health check failed")

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )

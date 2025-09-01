"""
FastAPI Communication Techniques Demo
Demonstrates: Long Polling, SSE, MQTT, WebSocket, Socket.IO
"""

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from contextlib import asynccontextmanager
import uvicorn
import logging
import asyncio
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, AsyncGenerator
import random
import uuid
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    logger.info("🚀 FastAPI Communication Demo Starting...")
    
    # Start background task for simulating updates
    update_task = asyncio.create_task(simulate_data_updates())
    
    yield
    
    # Clean up
    update_task.cancel()
    try:
        await update_task
    except asyncio.CancelledError:
        pass
    
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

# Long Polling State Management
class LongPollingManager:
    def __init__(self):
        self.data_updates = []
        self.last_update_time = time.time()
        self.polling_sessions = {}
        self.update_counter = 0
    
    async def add_update(self, update_data: Dict[Any, Any]):
        """Add a new update to the queue"""
        try:
            self.update_counter += 1
            update = {
                "id": self.update_counter,
                "data": update_data,
                "timestamp": datetime.now().isoformat(),
                "update_time": time.time()
            }
            self.data_updates.append(update)
            self.last_update_time = time.time()
            
            # Keep only last 50 updates
            if len(self.data_updates) > 50:
                self.data_updates = self.data_updates[-50:]
                
            logger.info(f"Added update #{self.update_counter}: {update_data}")
        except Exception as e:
            logger.error(f"Error adding update: {str(e)}")
    
    async def get_updates_since(self, last_update_id: int, timeout: int = 30) -> Dict[Any, Any]:
        """Get updates since the given update ID with timeout"""
        try:
            session_id = str(uuid.uuid4())
            self.polling_sessions[session_id] = time.time()
            
            start_time = time.time()
            
            while time.time() - start_time < timeout:
                # Get updates newer than last_update_id
                new_updates = [
                    update for update in self.data_updates 
                    if update["id"] > last_update_id
                ]
                
                if new_updates:
                    # Clean up session
                    self.polling_sessions.pop(session_id, None)
                    return {
                        "success": True,
                        "updates": new_updates,
                        "last_update_id": new_updates[-1]["id"],
                        "polling_time": time.time() - start_time,
                        "session_id": session_id
                    }
                
                # Check every 500ms
                await asyncio.sleep(0.5)
            
            # Timeout reached
            self.polling_sessions.pop(session_id, None)
            return {
                "success": True,
                "updates": [],
                "last_update_id": last_update_id,
                "polling_time": timeout,
                "timeout": True,
                "session_id": session_id
            }
            
        except Exception as e:
            logger.error(f"Error in long polling: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "updates": [],
                "last_update_id": last_update_id
            }
    
    def get_stats(self) -> Dict[str, Any]:
        """Get polling statistics"""
        try:
            return {
                "total_updates": len(self.data_updates),
                "last_update_time": self.last_update_time,
                "active_sessions": len(self.polling_sessions),
                "update_counter": self.update_counter
            }
        except Exception as e:
            logger.error(f"Error getting stats: {str(e)}")
            return {"error": str(e)}

# Initialize long polling manager
polling_manager = LongPollingManager()

# Server-Sent Events (SSE) State Management
class SSEManager:
    def __init__(self):
        self.active_connections = {}
        self.event_counter = 0
        self.message_queue = []
    
    async def add_connection(self, connection_id: str) -> AsyncGenerator[str, None]:
        """Add a new SSE connection and yield events"""
        try:
            self.active_connections[connection_id] = {
                "connected_at": time.time(),
                "last_ping": time.time()
            }
            
            logger.info(f"SSE connection added: {connection_id}")
            
            # Send initial connection event
            yield self.format_sse_message({
                "type": "connection",
                "message": "Connected to SSE stream",
                "connection_id": connection_id,
                "timestamp": datetime.now().isoformat()
            }, "connected")
            
            # Keep connection alive and send events
            while connection_id in self.active_connections:
                try:
                    # Send pending messages
                    for message in self.message_queue.copy():
                        yield self.format_sse_message(message["data"], message["event_type"])
                    
                    # Clear sent messages
                    self.message_queue.clear()
                    
                    # Send periodic heartbeat
                    current_time = time.time()
                    if current_time - self.active_connections[connection_id]["last_ping"] > 30:
                        yield self.format_sse_message({
                            "type": "heartbeat",
                            "timestamp": datetime.now().isoformat(),
                            "active_connections": len(self.active_connections)
                        }, "heartbeat")
                        self.active_connections[connection_id]["last_ping"] = current_time
                    
                    await asyncio.sleep(1)  # Check every second
                    
                except Exception as e:
                    logger.error(f"Error in SSE connection {connection_id}: {str(e)}")
                    break
                    
        except Exception as e:
            logger.error(f"Error managing SSE connection {connection_id}: {str(e)}")
        finally:
            self.remove_connection(connection_id)
    
    def remove_connection(self, connection_id: str):
        """Remove an SSE connection"""
        if connection_id in self.active_connections:
            del self.active_connections[connection_id]
            logger.info(f"SSE connection removed: {connection_id}")
    
    async def broadcast_message(self, data: Dict[Any, Any], event_type: str = "message"):
        """Broadcast a message to all active SSE connections"""
        try:
            self.event_counter += 1
            message = {
                "id": self.event_counter,
                "data": data,
                "event_type": event_type,
                "timestamp": datetime.now().isoformat()
            }
            
            self.message_queue.append(message)
            logger.info(f"SSE message queued: {event_type} - {data}")
            
        except Exception as e:
            logger.error(f"Error broadcasting SSE message: {str(e)}")
    
    def format_sse_message(self, data: Dict[Any, Any], event_type: str = "message") -> str:
        """Format data as SSE message"""
        try:
            self.event_counter += 1
            sse_data = {
                "id": self.event_counter,
                "data": data,
                "timestamp": datetime.now().isoformat()
            }
            
            # Format as SSE specification
            message = f"event: {event_type}\n"
            message += f"id: {self.event_counter}\n"
            message += f"data: {json.dumps(sse_data)}\n\n"
            
            return message
            
        except Exception as e:
            logger.error(f"Error formatting SSE message: {str(e)}")
            return f"event: error\ndata: {json.dumps({'error': str(e)})}\n\n"
    
    def get_stats(self) -> Dict[str, Any]:
        """Get SSE statistics"""
        try:
            return {
                "active_connections": len(self.active_connections),
                "event_counter": self.event_counter,
                "messages_queued": len(self.message_queue),
                "connections": {
                    conn_id: {
                        "connected_duration": time.time() - info["connected_at"],
                        "last_ping": info["last_ping"]
                    }
                    for conn_id, info in self.active_connections.items()
                }
            }
        except Exception as e:
            logger.error(f"Error getting SSE stats: {str(e)}")
            return {"error": str(e)}

# Initialize SSE manager
sse_manager = SSEManager()

async def simulate_data_updates():
    """Background task to simulate periodic data updates"""
    sample_updates = [
        {"type": "user_joined", "user": "Alice", "room": "general"},
        {"type": "message", "content": "Hello everyone!", "author": "Bob"},
        {"type": "status_change", "status": "online", "user": "Charlie"},
        {"type": "notification", "message": "New feature available", "priority": "high"},
        {"type": "data_sync", "records_updated": random.randint(1, 10)},
        {"type": "system_alert", "message": "Scheduled maintenance in 1 hour"},
    ]
    
    while True:
        try:
            # Random delay between 5-15 seconds
            await asyncio.sleep(random.uniform(5, 15))
            
            # Add random update
            update = random.choice(sample_updates).copy()
            update["timestamp"] = datetime.now().isoformat()
            
            # Send to both Long Polling and SSE
            await polling_manager.add_update(update)
            await sse_manager.broadcast_message(update, "data_update")
            
        except Exception as e:
            logger.error(f"Error in background updates: {str(e)}")
            await asyncio.sleep(5)

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
            "timestamp": datetime.now().isoformat(),
            "polling_stats": polling_manager.get_stats(),
            "sse_stats": sse_manager.get_stats()
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        raise HTTPException(status_code=500, detail="Health check failed")

# =============== LONG POLLING ENDPOINTS ===============

@app.get("/api/v1/poll")
async def long_poll(
    last_update_id: int = Query(0, description="ID of the last received update"),
    timeout: int = Query(30, ge=1, le=60, description="Polling timeout in seconds")
):
    """
    Long Polling Endpoint
    
    **Objective**: Implement long polling to reduce unnecessary HTTP requests 
    while maintaining near real-time updates.
    
    **How it works**: 
    - Client sends request with last known update ID
    - Server holds the request until new data is available or timeout occurs
    - Server responds immediately if new data exists, otherwise waits
    
    **Parameters**:
    - last_update_id: ID of the last update the client received (default: 0)
    - timeout: Maximum time to wait for updates in seconds (1-60, default: 30)
    
    **Response**:
    - success: Boolean indicating if the request was successful
    - updates: Array of new updates since last_update_id
    - last_update_id: ID of the most recent update
    - polling_time: Time spent waiting for updates
    - timeout: Boolean indicating if the request timed out
    """
    try:
        logger.info(f"Long polling request: last_update_id={last_update_id}, timeout={timeout}")
        
        result = await polling_manager.get_updates_since(last_update_id, timeout)
        
        logger.info(f"Long polling response: {len(result.get('updates', []))} updates")
        return result
        
    except Exception as e:
        logger.error(f"Long polling error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Long polling failed: {str(e)}")

@app.post("/api/v1/poll/trigger")
async def trigger_update(
    update_type: str = Query("manual", description="Type of update to trigger"),
    message: str = Query("Manual update triggered", description="Update message")
):
    """
    Trigger Manual Update
    
    **Objective**: Manually trigger an update for testing long polling functionality.
    
    **Parameters**:
    - update_type: Type of the update (default: "manual")
    - message: Custom message for the update
    
    **Response**:
    - success: Boolean indicating if update was triggered
    - update_id: ID of the created update
    - message: Confirmation message
    """
    try:
        update_data = {
            "type": update_type,
            "message": message,
            "triggered_by": "api",
            "timestamp": datetime.now().isoformat()
        }
        
        await polling_manager.add_update(update_data)
        
        return {
            "success": True,
            "update_id": polling_manager.update_counter,
            "message": f"Update triggered successfully",
            "data": update_data
        }
        
    except Exception as e:
        logger.error(f"Error triggering update: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to trigger update: {str(e)}")

@app.get("/api/v1/poll/stats")
async def get_polling_stats():
    """
    Get Long Polling Statistics
    
    **Objective**: Provide insights into the current state of the long polling system.
    
    **Response**:
    - total_updates: Total number of updates generated
    - last_update_time: Timestamp of the last update
    - active_sessions: Number of currently active polling sessions
    - update_counter: Current update counter value
    """
    try:
        stats = polling_manager.get_stats()
        return {
            "success": True,
            "stats": stats,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting polling stats: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get stats: {str(e)}")

@app.get("/api/v1/poll/history")
async def get_update_history(
    limit: int = Query(10, ge=1, le=50, description="Number of recent updates to return")
):
    """
    Get Update History
    
    **Objective**: Retrieve recent updates for debugging and testing purposes.
    
    **Parameters**:
    - limit: Maximum number of updates to return (1-50, default: 10)
    
    **Response**:
    - success: Boolean indicating success
    - updates: Array of recent updates
    - total_updates: Total number of updates in the system
    """
    try:
        recent_updates = polling_manager.data_updates[-limit:] if polling_manager.data_updates else []
        
        return {
            "success": True,
            "updates": recent_updates,
            "total_updates": len(polling_manager.data_updates),
            "requested_limit": limit,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting update history: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get history: {str(e)}")

# =============== SERVER-SENT EVENTS (SSE) ENDPOINTS ===============

@app.get("/api/v1/sse/stream")
async def sse_stream():
    """
    Server-Sent Events Stream Endpoint
    
    **Objective**: Establish a persistent streaming connection for real-time data updates.
    
    **How it works**:
    - Client opens EventSource connection to this endpoint
    - Server maintains persistent connection and streams events
    - Real-time push of data without client polling
    - Automatic reconnection support built into EventSource
    
    **Response**: 
    - Content-Type: text/event-stream
    - Events formatted according to SSE specification
    - Includes heartbeat for connection health
    """
    try:
        connection_id = str(uuid.uuid4())
        logger.info(f"New SSE connection: {connection_id}")
        
        def generate_events():
            return sse_manager.add_connection(connection_id)
        
        return StreamingResponse(
            generate_events(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Cache-Control"
            }
        )
        
    except Exception as e:
        logger.error(f"SSE stream error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"SSE stream failed: {str(e)}")

@app.post("/api/v1/sse/broadcast")
async def sse_broadcast(
    message: str = Query(..., description="Message to broadcast"),
    event_type: str = Query("custom", description="Type of event"),
    priority: str = Query("normal", description="Message priority (low, normal, high)")
):
    """
    Broadcast Message via SSE
    
    **Objective**: Send real-time messages to all connected SSE clients.
    
    **How it works**:
    - Accepts message and event type
    - Immediately broadcasts to all active SSE connections
    - No polling required - instant delivery
    
    **Parameters**:
    - message: The message content to broadcast
    - event_type: Type of event (default: "custom")
    - priority: Message priority level
    
    **Response**:
    - success: Boolean indicating if broadcast was successful
    - connections: Number of active connections that received the message
    - event_id: Unique identifier for the broadcasted event
    """
    try:
        broadcast_data = {
            "type": event_type,
            "message": message,
            "priority": priority,
            "broadcasted_by": "api",
            "timestamp": datetime.now().isoformat()
        }
        
        await sse_manager.broadcast_message(broadcast_data, event_type)
        
        return {
            "success": True,
            "connections": len(sse_manager.active_connections),
            "event_id": sse_manager.event_counter,
            "message": "Message broadcasted successfully",
            "data": broadcast_data
        }
        
    except Exception as e:
        logger.error(f"Error broadcasting SSE message: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to broadcast: {str(e)}")

@app.get("/api/v1/sse/stats")
async def get_sse_stats():
    """
    Get SSE Statistics
    
    **Objective**: Monitor Server-Sent Events system performance and active connections.
    
    **Response**:
    - active_connections: Number of currently connected SSE clients
    - event_counter: Total number of events sent
    - messages_queued: Number of pending messages
    - connections: Detailed information about each active connection
    """
    try:
        stats = sse_manager.get_stats()
        return {
            "success": True,
            "stats": stats,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting SSE stats: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get SSE stats: {str(e)}")

@app.post("/api/v1/sse/trigger")
async def trigger_sse_update(
    update_type: str = Query("sse_demo", description="Type of update to trigger"),
    message: str = Query("SSE update triggered", description="Update message")
):
    """
    Trigger SSE Update
    
    **Objective**: Manually trigger an SSE event for testing and demonstration.
    
    **Parameters**:
    - update_type: Type of the update (default: "sse_demo")
    - message: Custom message for the update
    
    **Response**:
    - success: Boolean indicating if update was triggered
    - connections: Number of active SSE connections
    - message: Confirmation message
    """
    try:
        update_data = {
            "type": update_type,
            "message": message,
            "triggered_by": "sse_api",
            "timestamp": datetime.now().isoformat()
        }
        
        # Send to both SSE and Long Polling for comparison
        await sse_manager.broadcast_message(update_data, "triggered_update")
        await polling_manager.add_update(update_data)
        
        return {
            "success": True,
            "sse_connections": len(sse_manager.active_connections),
            "polling_sessions": len(polling_manager.polling_sessions),
            "message": f"Update triggered for both SSE and Long Polling",
            "data": update_data
        }
        
    except Exception as e:
        logger.error(f"Error triggering SSE update: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to trigger SSE update: {str(e)}")

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )

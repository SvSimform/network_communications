"""
FastAPI Communication Techniques Demo
Demonstrates: Long Polling, SSE, MQTT, WebSocket, Socket.IO
"""

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks, WebSocket, WebSocketDisconnect
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
import os
import paho.mqtt.client as mqtt
import threading

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    logger.info("🚀 FastAPI Communication Demo Starting...")
    
    # Start background task for simulating updates
    update_task = asyncio.create_task(simulate_data_updates())
    
    # Initialize MQTT connection
    mqtt_manager.connect()
    
    yield
    
    # Clean up
    update_task.cancel()
    try:
        await update_task
    except asyncio.CancelledError:
        pass
    
    # Disconnect MQTT
    mqtt_manager.disconnect()
    
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

# MQTT Manager for Pub/Sub messaging
class MQTTManager:
    def __init__(self):
        self.broker_host = os.getenv("MQTT_BROKER_HOST", "localhost")
        self.broker_port = int(os.getenv("MQTT_BROKER_PORT", "1883"))
        self.client = None
        self.is_connected = False
        self.subscribers = {}
        self.published_messages = []
        self.message_counter = 0
        self.topics_stats = {}
        
    def on_connect(self, client, userdata, flags, rc):
        """Callback for MQTT connection"""
        try:
            if rc == 0:
                self.is_connected = True
                logger.info(f"🔗 MQTT connected to {self.broker_host}:{self.broker_port}")
                
                # Subscribe to demo topics
                demo_topics = [
                    "demo/updates",
                    "demo/notifications", 
                    "demo/alerts",
                    "demo/chat",
                    "demo/system"
                ]
                
                for topic in demo_topics:
                    client.subscribe(topic)
                    self.topics_stats[topic] = {"subscribed": True, "message_count": 0}
                    logger.info(f"📡 Subscribed to topic: {topic}")
                    
            else:
                logger.error(f"❌ MQTT connection failed with code {rc}")
                self.is_connected = False
        except Exception as e:
            logger.error(f"Error in MQTT on_connect: {str(e)}")
    
    def on_disconnect(self, client, userdata, rc):
        """Callback for MQTT disconnection"""
        self.is_connected = False
        logger.warning(f"🔌 MQTT disconnected with code {rc}")
    
    def on_message(self, client, userdata, msg):
        """Callback for received MQTT messages"""
        try:
            topic = msg.topic
            payload = msg.payload.decode('utf-8')
            
            # Update topic statistics
            if topic in self.topics_stats:
                self.topics_stats[topic]["message_count"] += 1
            
            message_data = {
                "topic": topic,
                "payload": payload,
                "timestamp": datetime.now().isoformat(),
                "qos": msg.qos,
                "retain": msg.retain
            }
            
            logger.info(f"📨 MQTT message received on {topic}: {payload}")
            
            # Store message for retrieval
            self.message_counter += 1
            stored_message = {
                "id": self.message_counter,
                "data": message_data,
                "received_at": time.time()
            }
            
            self.published_messages.append(stored_message)
            
            # Keep only last 100 messages
            if len(self.published_messages) > 100:
                self.published_messages = self.published_messages[-100:]
            
            # Forward to other communication channels for cross-system demo
            asyncio.create_task(self._forward_to_other_systems(message_data))
            
        except Exception as e:
            logger.error(f"Error processing MQTT message: {str(e)}")
    
    async def _forward_to_other_systems(self, mqtt_message):
        """Forward MQTT messages to Long Polling and SSE for demonstration"""
        try:
            forward_data = {
                "type": "mqtt_message",
                "mqtt_topic": mqtt_message["topic"],
                "mqtt_payload": mqtt_message["payload"],
                "source": "mqtt",
                "timestamp": mqtt_message["timestamp"]
            }
            
            # Send to Long Polling
            await polling_manager.add_update(forward_data)
            
            # Send to SSE
            await sse_manager.broadcast_message(forward_data, "mqtt_message")
            
        except Exception as e:
            logger.error(f"Error forwarding MQTT message: {str(e)}")
    
    def connect(self):
        """Connect to MQTT broker"""
        try:
            if self.client is None:
                self.client = mqtt.Client()
                self.client.on_connect = self.on_connect
                self.client.on_disconnect = self.on_disconnect
                self.client.on_message = self.on_message
            
            if not self.is_connected:
                logger.info(f"🔄 Connecting to MQTT broker at {self.broker_host}:{self.broker_port}")
                self.client.connect(self.broker_host, self.broker_port, 60)
                self.client.loop_start()
                
        except Exception as e:
            logger.error(f"Error connecting to MQTT: {str(e)}")
            self.is_connected = False
    
    def disconnect(self):
        """Disconnect from MQTT broker"""
        try:
            if self.client and self.is_connected:
                self.client.loop_stop()
                self.client.disconnect()
                self.is_connected = False
                logger.info("🔌 MQTT disconnected")
        except Exception as e:
            logger.error(f"Error disconnecting MQTT: {str(e)}")
    
    def publish_message(self, topic: str, payload: str, qos: int = 0, retain: bool = False):
        """Publish message to MQTT topic"""
        try:
            if not self.is_connected:
                self.connect()
                # Wait a moment for connection
                time.sleep(1)
            
            if self.is_connected:
                result = self.client.publish(topic, payload, qos, retain)
                
                if result.rc == mqtt.MQTT_ERR_SUCCESS:
                    logger.info(f"📤 MQTT message published to {topic}: {payload}")
                    
                    # Store published message
                    self.message_counter += 1
                    published_msg = {
                        "id": self.message_counter,
                        "type": "published",
                        "topic": topic,
                        "payload": payload,
                        "qos": qos,
                        "retain": retain,
                        "timestamp": datetime.now().isoformat(),
                        "published_at": time.time()
                    }
                    
                    self.published_messages.append(published_msg)
                    
                    return True
                else:
                    logger.error(f"❌ Failed to publish MQTT message: {result.rc}")
                    return False
            else:
                logger.error("❌ MQTT not connected, cannot publish")
                return False
                
        except Exception as e:
            logger.error(f"Error publishing MQTT message: {str(e)}")
            return False
    
    def subscribe_to_topic(self, topic: str, qos: int = 0):
        """Subscribe to MQTT topic"""
        try:
            if not self.is_connected:
                self.connect()
                time.sleep(1)
            
            if self.is_connected:
                result = self.client.subscribe(topic, qos)
                
                if result[0] == mqtt.MQTT_ERR_SUCCESS:
                    self.topics_stats[topic] = {"subscribed": True, "message_count": 0}
                    logger.info(f"📡 Subscribed to MQTT topic: {topic}")
                    return True
                else:
                    logger.error(f"❌ Failed to subscribe to topic {topic}: {result[0]}")
                    return False
            else:
                logger.error("❌ MQTT not connected, cannot subscribe")
                return False
                
        except Exception as e:
            logger.error(f"Error subscribing to MQTT topic: {str(e)}")
            return False
    
    def unsubscribe_from_topic(self, topic: str):
        """Unsubscribe from MQTT topic"""
        try:
            if self.is_connected:
                result = self.client.unsubscribe(topic)
                
                if result[0] == mqtt.MQTT_ERR_SUCCESS:
                    if topic in self.topics_stats:
                        self.topics_stats[topic]["subscribed"] = False
                    logger.info(f"📡 Unsubscribed from MQTT topic: {topic}")
                    return True
                else:
                    logger.error(f"❌ Failed to unsubscribe from topic {topic}: {result[0]}")
                    return False
            else:
                logger.error("❌ MQTT not connected, cannot unsubscribe")
                return False
                
        except Exception as e:
            logger.error(f"Error unsubscribing from MQTT topic: {str(e)}")
            return False
    
    def get_stats(self):
        """Get MQTT statistics"""
        try:
            return {
                "connected": self.is_connected,
                "broker": f"{self.broker_host}:{self.broker_port}",
                "total_messages": len(self.published_messages),
                "message_counter": self.message_counter,
                "topics": self.topics_stats,
                "recent_messages": self.published_messages[-10:] if self.published_messages else []
            }
        except Exception as e:
            logger.error(f"Error getting MQTT stats: {str(e)}")
            return {"error": str(e)}

# Initialize MQTT manager
mqtt_manager = MQTTManager()

# WebSocket Manager for bidirectional real-time communication
class WebSocketManager:
    def __init__(self):
        self.active_connections = {}
        self.rooms = {}  # Room-based connections
        self.message_counter = 0
        self.connection_counter = 0
    
    async def connect(self, websocket: WebSocket, client_id: str = None, room: str = "general"):
        """Accept and manage WebSocket connection"""
        try:
            await websocket.accept()
            
            if client_id is None:
                client_id = f"client_{self.connection_counter}"
                self.connection_counter += 1
            
            # Store connection info
            connection_info = {
                "websocket": websocket,
                "client_id": client_id,
                "room": room,
                "connected_at": time.time(),
                "last_ping": time.time(),
                "messages_sent": 0,
                "messages_received": 0
            }
            
            self.active_connections[client_id] = connection_info
            
            # Add to room
            if room not in self.rooms:
                self.rooms[room] = set()
            self.rooms[room].add(client_id)
            
            logger.info(f"🔌 WebSocket connected: {client_id} in room '{room}'")
            
            # Send welcome message
            await self.send_personal_message({
                "type": "connection",
                "message": f"Connected as {client_id}",
                "room": room,
                "timestamp": datetime.now().isoformat()
            }, client_id)
            
            # Notify room about new connection
            await self.broadcast_to_room({
                "type": "user_joined",
                "user": client_id,
                "room": room,
                "timestamp": datetime.now().isoformat(),
                "total_connections": len(self.active_connections)
            }, room, exclude=client_id)
            
            return client_id
            
        except Exception as e:
            logger.error(f"Error connecting WebSocket: {str(e)}")
            return None
    
    def disconnect(self, client_id: str):
        """Remove WebSocket connection"""
        try:
            if client_id in self.active_connections:
                connection_info = self.active_connections[client_id]
                room = connection_info["room"]
                
                # Remove from room
                if room in self.rooms and client_id in self.rooms[room]:
                    self.rooms[room].remove(client_id)
                    
                    # Clean up empty rooms
                    if not self.rooms[room]:
                        del self.rooms[room]
                
                # Remove connection
                del self.active_connections[client_id]
                
                logger.info(f"🔌 WebSocket disconnected: {client_id} from room '{room}'")
                
                # Notify room about disconnection (best effort)
                asyncio.create_task(self.broadcast_to_room({
                    "type": "user_left",
                    "user": client_id,
                    "room": room,
                    "timestamp": datetime.now().isoformat(),
                    "total_connections": len(self.active_connections)
                }, room))
                
        except Exception as e:
            logger.error(f"Error disconnecting WebSocket {client_id}: {str(e)}")
    
    async def send_personal_message(self, message: Dict[Any, Any], client_id: str):
        """Send message to specific client"""
        try:
            if client_id in self.active_connections:
                connection_info = self.active_connections[client_id]
                websocket = connection_info["websocket"]
                
                self.message_counter += 1
                formatted_message = {
                    "id": self.message_counter,
                    "data": message,
                    "timestamp": datetime.now().isoformat(),
                    "recipient": client_id
                }
                
                await websocket.send_text(json.dumps(formatted_message))
                connection_info["messages_sent"] += 1
                
                logger.info(f"📤 WebSocket message sent to {client_id}: {message.get('type', 'unknown')}")
                return True
            else:
                logger.warning(f"⚠️ Client {client_id} not connected")
                return False
                
        except Exception as e:
            logger.error(f"Error sending WebSocket message to {client_id}: {str(e)}")
            # Remove broken connection
            self.disconnect(client_id)
            return False
    
    async def broadcast_to_room(self, message: Dict[Any, Any], room: str, exclude: str = None):
        """Broadcast message to all clients in a room"""
        try:
            if room not in self.rooms:
                logger.warning(f"⚠️ Room '{room}' does not exist")
                return 0
            
            clients_in_room = self.rooms[room].copy()
            if exclude:
                clients_in_room.discard(exclude)
            
            sent_count = 0
            failed_clients = []
            
            for client_id in clients_in_room:
                success = await self.send_personal_message(message, client_id)
                if success:
                    sent_count += 1
                else:
                    failed_clients.append(client_id)
            
            # Clean up failed connections
            for failed_client in failed_clients:
                self.disconnect(failed_client)
            
            logger.info(f"📡 WebSocket broadcast to room '{room}': {sent_count} clients")
            return sent_count
            
        except Exception as e:
            logger.error(f"Error broadcasting to room {room}: {str(e)}")
            return 0
    
    async def broadcast_to_all(self, message: Dict[Any, Any]):
        """Broadcast message to all connected clients"""
        try:
            all_clients = list(self.active_connections.keys())
            sent_count = 0
            failed_clients = []
            
            for client_id in all_clients:
                success = await self.send_personal_message(message, client_id)
                if success:
                    sent_count += 1
                else:
                    failed_clients.append(client_id)
            
            # Clean up failed connections
            for failed_client in failed_clients:
                self.disconnect(failed_client)
            
            logger.info(f"📡 WebSocket broadcast to all: {sent_count} clients")
            return sent_count
            
        except Exception as e:
            logger.error(f"Error broadcasting to all WebSocket clients: {str(e)}")
            return 0
    
    async def handle_client_message(self, client_id: str, message: str):
        """Process incoming message from client"""
        try:
            if client_id not in self.active_connections:
                return False
            
            connection_info = self.active_connections[client_id]
            connection_info["messages_received"] += 1
            connection_info["last_ping"] = time.time()
            
            # Parse message
            try:
                parsed_message = json.loads(message)
            except json.JSONDecodeError:
                # Treat as plain text message
                parsed_message = {
                    "type": "text",
                    "content": message
                }
            
            # Add metadata
            parsed_message["from"] = client_id
            parsed_message["room"] = connection_info["room"]
            parsed_message["timestamp"] = datetime.now().isoformat()
            
            logger.info(f"📥 WebSocket message from {client_id}: {parsed_message.get('type', 'unknown')}")
            
            # Handle different message types
            message_type = parsed_message.get("type", "text")
            
            if message_type == "ping":
                # Respond with pong
                await self.send_personal_message({
                    "type": "pong",
                    "timestamp": datetime.now().isoformat()
                }, client_id)
                
            elif message_type == "join_room":
                # Change room
                new_room = parsed_message.get("room", "general")
                await self.change_room(client_id, new_room)
                
            elif message_type == "broadcast":
                # Broadcast to room
                broadcast_data = {
                    "type": "broadcast",
                    "content": parsed_message.get("content", ""),
                    "from": client_id,
                    "timestamp": datetime.now().isoformat()
                }
                await self.broadcast_to_room(broadcast_data, connection_info["room"], exclude=client_id)
                
                # Also forward to other communication systems for cross-system demo
                await self._forward_to_other_systems(broadcast_data)
                
            else:
                # Default: broadcast as chat message
                chat_data = {
                    "type": "chat",
                    "content": parsed_message.get("content", message),
                    "from": client_id,
                    "room": connection_info["room"],
                    "timestamp": datetime.now().isoformat()
                }
                await self.broadcast_to_room(chat_data, connection_info["room"], exclude=client_id)
                
                # Forward to other systems
                await self._forward_to_other_systems(chat_data)
            
            return True
            
        except Exception as e:
            logger.error(f"Error handling WebSocket message from {client_id}: {str(e)}")
            return False
    
    async def change_room(self, client_id: str, new_room: str):
        """Move client to different room"""
        try:
            if client_id not in self.active_connections:
                return False
            
            connection_info = self.active_connections[client_id]
            old_room = connection_info["room"]
            
            if old_room == new_room:
                return True
            
            # Remove from old room
            if old_room in self.rooms and client_id in self.rooms[old_room]:
                self.rooms[old_room].remove(client_id)
                
                # Notify old room
                await self.broadcast_to_room({
                    "type": "user_left",
                    "user": client_id,
                    "room": old_room,
                    "moved_to": new_room,
                    "timestamp": datetime.now().isoformat()
                }, old_room)
                
                # Clean up empty room
                if not self.rooms[old_room]:
                    del self.rooms[old_room]
            
            # Add to new room
            if new_room not in self.rooms:
                self.rooms[new_room] = set()
            self.rooms[new_room].add(client_id)
            connection_info["room"] = new_room
            
            # Notify client
            await self.send_personal_message({
                "type": "room_changed",
                "old_room": old_room,
                "new_room": new_room,
                "timestamp": datetime.now().isoformat()
            }, client_id)
            
            # Notify new room
            await self.broadcast_to_room({
                "type": "user_joined",
                "user": client_id,
                "room": new_room,
                "moved_from": old_room,
                "timestamp": datetime.now().isoformat()
            }, new_room, exclude=client_id)
            
            logger.info(f"🏠 Client {client_id} moved from '{old_room}' to '{new_room}'")
            return True
            
        except Exception as e:
            logger.error(f"Error changing room for {client_id}: {str(e)}")
            return False
    
    async def _forward_to_other_systems(self, websocket_message):
        """Forward WebSocket messages to other communication systems for demonstration"""
        try:
            forward_data = {
                "type": "websocket_message",
                "websocket_type": websocket_message.get("type", "unknown"),
                "content": websocket_message.get("content", ""),
                "from": websocket_message.get("from", "unknown"),
                "room": websocket_message.get("room", "general"),
                "source": "websocket",
                "timestamp": websocket_message.get("timestamp", datetime.now().isoformat())
            }
            
            # Send to Long Polling
            await polling_manager.add_update(forward_data)
            
            # Send to SSE
            await sse_manager.broadcast_message(forward_data, "websocket_message")
            
            # Send to MQTT
            mqtt_topic = f"websocket/{websocket_message.get('room', 'general')}"
            mqtt_payload = json.dumps(forward_data)
            mqtt_manager.publish_message(mqtt_topic, mqtt_payload)
            
        except Exception as e:
            logger.error(f"Error forwarding WebSocket message: {str(e)}")
    
    def get_stats(self):
        """Get WebSocket statistics"""
        try:
            room_stats = {}
            for room, clients in self.rooms.items():
                room_stats[room] = {
                    "client_count": len(clients),
                    "clients": list(clients)
                }
            
            connection_stats = {}
            for client_id, info in self.active_connections.items():
                connection_stats[client_id] = {
                    "room": info["room"],
                    "connected_duration": time.time() - info["connected_at"],
                    "messages_sent": info["messages_sent"],
                    "messages_received": info["messages_received"],
                    "last_activity": time.time() - info["last_ping"]
                }
            
            return {
                "total_connections": len(self.active_connections),
                "total_rooms": len(self.rooms),
                "message_counter": self.message_counter,
                "connection_counter": self.connection_counter,
                "rooms": room_stats,
                "connections": connection_stats
            }
            
        except Exception as e:
            logger.error(f"Error getting WebSocket stats: {str(e)}")
            return {"error": str(e)}

# Initialize WebSocket manager
websocket_manager = WebSocketManager()

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
    
    mqtt_topics = ["demo/updates", "demo/notifications", "demo/alerts", "demo/chat", "demo/system"]
    
    while True:
        try:
            # Random delay between 5-15 seconds
            await asyncio.sleep(random.uniform(5, 15))
            
            # Add random update
            update = random.choice(sample_updates).copy()
            update["timestamp"] = datetime.now().isoformat()
            
            # Send to Long Polling and SSE
            await polling_manager.add_update(update)
            await sse_manager.broadcast_message(update, "data_update")
            
            # Send to MQTT (publish to random topic)
            mqtt_topic = random.choice(mqtt_topics)
            mqtt_payload = json.dumps(update)
            mqtt_manager.publish_message(mqtt_topic, mqtt_payload)
            
            # Send to WebSocket (broadcast to all rooms)
            await websocket_manager.broadcast_to_all({
                "type": "background_update",
                "data": update,
                "source": "background_task",
                "timestamp": update["timestamp"]
            })
            
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
            "sse_stats": sse_manager.get_stats(),
            "mqtt_stats": mqtt_manager.get_stats(),
            "websocket_stats": websocket_manager.get_stats()
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

# =============== MQTT ENDPOINTS ===============

@app.post("/api/v1/mqtt/publish")
async def mqtt_publish(
    topic: str = Query(..., description="MQTT topic to publish to"),
    message: str = Query(..., description="Message payload"),
    qos: int = Query(0, ge=0, le=2, description="Quality of Service level (0, 1, or 2)"),
    retain: bool = Query(False, description="Retain message flag")
):
    """
    Publish Message to MQTT Topic
    
    **Objective**: Send messages to MQTT broker for pub/sub communication pattern.
    
    **How it works**:
    - Publishes message to specified MQTT topic
    - Uses configurable QoS (Quality of Service) levels
    - Supports message retention
    - Cross-system integration with Long Polling and SSE
    
    **Parameters**:
    - topic: MQTT topic name (e.g., "demo/notifications")
    - message: Message payload to publish
    - qos: Quality of Service (0=at most once, 1=at least once, 2=exactly once)
    - retain: Whether broker should retain this message for new subscribers
    
    **Response**:
    - success: Boolean indicating if message was published
    - topic: Topic where message was published
    - message_id: Unique identifier for the message
    """
    try:
        success = mqtt_manager.publish_message(topic, message, qos, retain)
        
        if success:
            # Also broadcast to other systems for cross-communication demo
            broadcast_data = {
                "type": "mqtt_publish",
                "topic": topic,
                "message": message,
                "qos": qos,
                "retain": retain,
                "published_by": "api",
                "timestamp": datetime.now().isoformat()
            }
            
            await polling_manager.add_update(broadcast_data)
            await sse_manager.broadcast_message(broadcast_data, "mqtt_publish")
            
            return {
                "success": True,
                "topic": topic,
                "message": message,
                "qos": qos,
                "retain": retain,
                "message_id": mqtt_manager.message_counter,
                "broker": f"{mqtt_manager.broker_host}:{mqtt_manager.broker_port}"
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to publish MQTT message")
            
    except Exception as e:
        logger.error(f"Error publishing MQTT message: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to publish MQTT message: {str(e)}")

@app.post("/api/v1/mqtt/subscribe")
async def mqtt_subscribe(
    topic: str = Query(..., description="MQTT topic to subscribe to"),
    qos: int = Query(0, ge=0, le=2, description="Quality of Service level")
):
    """
    Subscribe to MQTT Topic
    
    **Objective**: Subscribe to MQTT topics for receiving messages.
    
    **Parameters**:
    - topic: MQTT topic pattern to subscribe to (supports wildcards + and #)
    - qos: Maximum Quality of Service level for subscription
    
    **Response**:
    - success: Boolean indicating if subscription was successful
    - topic: Topic pattern subscribed to
    - qos: QoS level for the subscription
    """
    try:
        success = mqtt_manager.subscribe_to_topic(topic, qos)
        
        if success:
            return {
                "success": True,
                "topic": topic,
                "qos": qos,
                "message": f"Successfully subscribed to topic: {topic}",
                "broker": f"{mqtt_manager.broker_host}:{mqtt_manager.broker_port}"
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to subscribe to MQTT topic")
            
    except Exception as e:
        logger.error(f"Error subscribing to MQTT topic: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to subscribe to MQTT topic: {str(e)}")

@app.post("/api/v1/mqtt/unsubscribe")
async def mqtt_unsubscribe(
    topic: str = Query(..., description="MQTT topic to unsubscribe from")
):
    """
    Unsubscribe from MQTT Topic
    
    **Objective**: Unsubscribe from MQTT topics to stop receiving messages.
    
    **Parameters**:
    - topic: MQTT topic pattern to unsubscribe from
    
    **Response**:
    - success: Boolean indicating if unsubscription was successful
    - topic: Topic pattern unsubscribed from
    """
    try:
        success = mqtt_manager.unsubscribe_from_topic(topic)
        
        if success:
            return {
                "success": True,
                "topic": topic,
                "message": f"Successfully unsubscribed from topic: {topic}",
                "broker": f"{mqtt_manager.broker_host}:{mqtt_manager.broker_port}"
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to unsubscribe from MQTT topic")
            
    except Exception as e:
        logger.error(f"Error unsubscribing from MQTT topic: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to unsubscribe from MQTT topic: {str(e)}")

@app.get("/api/v1/mqtt/stats")
async def get_mqtt_stats():
    """
    Get MQTT Statistics
    
    **Objective**: Monitor MQTT system performance, connections, and message flow.
    
    **Response**:
    - connected: Boolean indicating if connected to MQTT broker
    - broker: Broker host and port information
    - total_messages: Total number of messages processed
    - topics: Statistics for each subscribed topic
    - recent_messages: Last 10 messages (published and received)
    """
    try:
        stats = mqtt_manager.get_stats()
        return {
            "success": True,
            "stats": stats,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting MQTT stats: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get MQTT stats: {str(e)}")

@app.get("/api/v1/mqtt/messages")
async def get_mqtt_messages(
    limit: int = Query(20, ge=1, le=100, description="Number of recent messages to return"),
    topic_filter: Optional[str] = Query(None, description="Filter messages by topic")
):
    """
    Get Recent MQTT Messages
    
    **Objective**: Retrieve recent MQTT messages for debugging and monitoring.
    
    **Parameters**:
    - limit: Maximum number of messages to return (1-100, default: 20)
    - topic_filter: Optional topic filter to show messages from specific topic
    
    **Response**:
    - success: Boolean indicating success
    - messages: Array of recent MQTT messages
    - total_messages: Total number of messages in the system
    """
    try:
        messages = mqtt_manager.published_messages
        
        # Filter by topic if specified
        if topic_filter:
            messages = [msg for msg in messages 
                       if msg.get("topic", "").startswith(topic_filter) or 
                          (msg.get("data", {}).get("topic", "")).startswith(topic_filter)]
        
        # Apply limit
        recent_messages = messages[-limit:] if messages else []
        
        return {
            "success": True,
            "messages": recent_messages,
            "total_messages": len(mqtt_manager.published_messages),
            "requested_limit": limit,
            "topic_filter": topic_filter,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting MQTT messages: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get MQTT messages: {str(e)}")

@app.post("/api/v1/mqtt/demo")
async def mqtt_demo_message(
    demo_type: str = Query("notification", description="Type of demo message"),
    message: str = Query("MQTT demo message", description="Demo message content")
):
    """
    Send Demo MQTT Message
    
    **Objective**: Send demonstration messages to showcase MQTT pub/sub functionality.
    
    **Parameters**:
    - demo_type: Type of demo (notification, alert, chat, system, update)
    - message: Content of the demo message
    
    **Response**:
    - success: Boolean indicating if demo was successful
    - published_topics: List of topics where message was published
    - cross_system: Information about cross-system broadcasting
    """
    try:
        # Map demo types to topics
        topic_mapping = {
            "notification": "demo/notifications",
            "alert": "demo/alerts", 
            "chat": "demo/chat",
            "system": "demo/system",
            "update": "demo/updates"
        }
        
        topic = topic_mapping.get(demo_type, "demo/notifications")
        
        demo_data = {
            "type": demo_type,
            "message": message,
            "demo": True,
            "timestamp": datetime.now().isoformat(),
            "id": str(uuid.uuid4())
        }
        
        # Publish to MQTT
        success = mqtt_manager.publish_message(topic, json.dumps(demo_data))
        
        if success:
            # Also send to other communication systems for comparison
            cross_system_data = {
                "type": "mqtt_demo",
                "demo_type": demo_type,
                "mqtt_topic": topic,
                "message": message,
                "source": "mqtt_demo_api",
                "timestamp": datetime.now().isoformat()
            }
            
            await polling_manager.add_update(cross_system_data)
            await sse_manager.broadcast_message(cross_system_data, "mqtt_demo")
            
            return {
                "success": True,
                "published_topics": [topic],
                "demo_type": demo_type,
                "message": message,
                "cross_system": {
                    "long_polling": True,
                    "sse": True,
                    "mqtt": True
                },
                "mqtt_message_id": mqtt_manager.message_counter,
                "timestamp": demo_data["timestamp"]
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to publish demo MQTT message")
            
    except Exception as e:
        logger.error(f"Error sending MQTT demo message: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to send MQTT demo message: {str(e)}")

# =============== WEBSOCKET ENDPOINTS ===============

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, client_id: str = None, room: str = "general"):
    """
    WebSocket Connection Endpoint
    
    **Objective**: Establish bidirectional real-time communication using WebSocket protocol.
    
    **How it works**:
    - Client connects via WebSocket protocol (ws:// or wss://)
    - Full-duplex communication: both client and server can send messages anytime
    - Room-based messaging for organized communication
    - Automatic connection management and cleanup
    
    **Parameters**:
    - client_id: Optional unique identifier for the client
    - room: Room name to join (default: "general")
    
    **Features**:
    - Real-time bidirectional messaging
    - Room-based chat and broadcasts
    - Cross-system message forwarding
    - Connection state management
    - Ping/pong heartbeat support
    """
    connection_id = await websocket_manager.connect(websocket, client_id, room)
    
    if connection_id is None:
        return
    
    try:
        while True:
            # Wait for messages from client
            data = await websocket.receive_text()
            
            # Process the message
            await websocket_manager.handle_client_message(connection_id, data)
            
    except WebSocketDisconnect:
        websocket_manager.disconnect(connection_id)
        logger.info(f"🔌 WebSocket client {connection_id} disconnected normally")
    except Exception as e:
        logger.error(f"🔌 WebSocket error for client {connection_id}: {str(e)}")
        websocket_manager.disconnect(connection_id)

@app.get("/api/v1/websocket/stats")
async def get_websocket_stats():
    """
    Get WebSocket Statistics
    
    **Objective**: Monitor WebSocket system performance, connections, and activity.
    
    **Response**:
    - total_connections: Number of active WebSocket connections
    - total_rooms: Number of active rooms
    - message_counter: Total messages processed
    - rooms: Detailed information about each room
    - connections: Connection details for each client
    """
    try:
        stats = websocket_manager.get_stats()
        return {
            "success": True,
            "stats": stats,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting WebSocket stats: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get WebSocket stats: {str(e)}")

@app.post("/api/v1/websocket/broadcast")
async def websocket_broadcast(
    message: str = Query(..., description="Message to broadcast"),
    room: str = Query("general", description="Room to broadcast to (or 'all' for all rooms)"),
    message_type: str = Query("announcement", description="Type of message")
):
    """
    Broadcast Message via WebSocket
    
    **Objective**: Send real-time messages to WebSocket clients in specific rooms or all rooms.
    
    **Parameters**:
    - message: The message content to broadcast
    - room: Target room name, or "all" to broadcast to all connected clients
    - message_type: Type of message (announcement, alert, notification, etc.)
    
    **Response**:
    - success: Boolean indicating if broadcast was successful
    - clients_reached: Number of clients that received the message
    - rooms_targeted: List of rooms where message was sent
    """
    try:
        broadcast_data = {
            "type": message_type,
            "content": message,
            "from": "api",
            "timestamp": datetime.now().isoformat(),
            "broadcast": True
        }
        
        if room.lower() == "all":
            clients_reached = await websocket_manager.broadcast_to_all(broadcast_data)
            rooms_targeted = list(websocket_manager.rooms.keys())
        else:
            clients_reached = await websocket_manager.broadcast_to_room(broadcast_data, room)
            rooms_targeted = [room] if room in websocket_manager.rooms else []
        
        # Also forward to other communication systems for cross-system demo
        forward_data = {
            "type": "websocket_broadcast",
            "message_type": message_type,
            "content": message,
            "room": room,
            "source": "websocket_api",
            "timestamp": datetime.now().isoformat()
        }
        
        await polling_manager.add_update(forward_data)
        await sse_manager.broadcast_message(forward_data, "websocket_broadcast")
        
        mqtt_topic = f"websocket/broadcast/{room}" if room != "all" else "websocket/broadcast/all"
        mqtt_payload = json.dumps(forward_data)
        mqtt_manager.publish_message(mqtt_topic, mqtt_payload)
        
        return {
            "success": True,
            "clients_reached": clients_reached,
            "rooms_targeted": rooms_targeted,
            "message": message,
            "message_type": message_type,
            "cross_system": {
                "long_polling": True,
                "sse": True,
                "mqtt": True,
                "websocket": True
            }
        }
        
    except Exception as e:
        logger.error(f"Error broadcasting WebSocket message: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to broadcast WebSocket message: {str(e)}")

@app.post("/api/v1/websocket/send")
async def websocket_send_personal(
    client_id: str = Query(..., description="Target client ID"),
    message: str = Query(..., description="Message to send"),
    message_type: str = Query("direct", description="Type of message")
):
    """
    Send Personal Message via WebSocket
    
    **Objective**: Send direct messages to specific WebSocket clients.
    
    **Parameters**:
    - client_id: ID of the target client
    - message: The message content to send
    - message_type: Type of message (direct, notification, etc.)
    
    **Response**:
    - success: Boolean indicating if message was sent
    - client_id: Target client ID
    - delivered: Boolean indicating if client received the message
    """
    try:
        message_data = {
            "type": message_type,
            "content": message,
            "from": "api",
            "timestamp": datetime.now().isoformat(),
            "direct": True
        }
        
        delivered = await websocket_manager.send_personal_message(message_data, client_id)
        
        return {
            "success": True,
            "client_id": client_id,
            "message": message,
            "message_type": message_type,
            "delivered": delivered,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error sending personal WebSocket message: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to send personal WebSocket message: {str(e)}")

@app.get("/api/v1/websocket/rooms")
async def get_websocket_rooms():
    """
    Get WebSocket Rooms Information
    
    **Objective**: List all active WebSocket rooms and their participants.
    
    **Response**:
    - success: Boolean indicating success
    - rooms: Dictionary of rooms with client lists
    - total_rooms: Number of active rooms
    - total_clients: Total number of connected clients
    """
    try:
        stats = websocket_manager.get_stats()
        
        return {
            "success": True,
            "rooms": stats.get("rooms", {}),
            "total_rooms": stats.get("total_rooms", 0),
            "total_clients": stats.get("total_connections", 0),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting WebSocket rooms: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get WebSocket rooms: {str(e)}")

@app.post("/api/v1/websocket/demo")
async def websocket_demo_message(
    demo_type: str = Query("chat", description="Type of demo message"),
    message: str = Query("WebSocket demo message", description="Demo message content"),
    room: str = Query("general", description="Target room for demo")
):
    """
    Send Demo WebSocket Message
    
    **Objective**: Send demonstration messages to showcase WebSocket real-time functionality.
    
    **Parameters**:
    - demo_type: Type of demo (chat, notification, alert, system, announcement)
    - message: Content of the demo message
    - room: Target room for the demo message
    
    **Response**:
    - success: Boolean indicating if demo was successful
    - clients_reached: Number of clients that received the message
    - cross_system: Information about cross-system broadcasting
    """
    try:
        demo_data = {
            "type": demo_type,
            "content": message,
            "from": "demo_api",
            "room": room,
            "demo": True,
            "timestamp": datetime.now().isoformat(),
            "id": str(uuid.uuid4())
        }
        
        # Send via WebSocket
        clients_reached = await websocket_manager.broadcast_to_room(demo_data, room)
        
        # Also send to other communication systems for comparison
        cross_system_data = {
            "type": "websocket_demo",
            "demo_type": demo_type,
            "content": message,
            "room": room,
            "source": "websocket_demo_api",
            "timestamp": datetime.now().isoformat()
        }
        
        await polling_manager.add_update(cross_system_data)
        await sse_manager.broadcast_message(cross_system_data, "websocket_demo")
        
        mqtt_topic = f"websocket/demo/{room}"
        mqtt_payload = json.dumps(cross_system_data)
        mqtt_manager.publish_message(mqtt_topic, mqtt_payload)
        
        return {
            "success": True,
            "clients_reached": clients_reached,
            "room": room,
            "demo_type": demo_type,
            "message": message,
            "cross_system": {
                "long_polling": True,
                "sse": True,
                "mqtt": True,
                "websocket": True
            },
            "websocket_message_id": websocket_manager.message_counter,
            "timestamp": demo_data["timestamp"]
        }
        
    except Exception as e:
        logger.error(f"Error sending WebSocket demo message: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to send WebSocket demo message: {str(e)}")

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )

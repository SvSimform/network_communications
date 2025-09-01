# Communication Techniques Demo

A FastAPI application demonstrating Long Polling communication technique.

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose

### Running the Application

1. **Start the application:**
   ```bash
   docker compose up --build -d
   ```

2. **Access the application:**
   - FastAPI App: http://localhost:8000
   - API Documentation: http://localhost:8000/docs

## 📋 Current Status

### ✅ Implemented
**Long Polling:**
- Efficient real-time communication with 83% fewer HTTP requests
- Configurable timeout (1-60 seconds)
- Background data simulation
- Manual update triggers  
- Statistics and monitoring

**Server-Sent Events (SSE):**
- Real-time event streaming
- Persistent connection with auto-reconnection
- Event broadcasting to multiple clients
- Custom event types and priorities
- Connection management and statistics

### 🚧 Coming Next
- MQTT integration
- WebSocket implementation
- Socket.IO integration

## 🎯 API Testing

**See [LONG_POLLING_DEMO.md](LONG_POLLING_DEMO.md) for Long Polling demo**
**See [SSE_DEMO.md](SSE_DEMO.md) for Server-Sent Events demo**

### Long Polling Test Commands:

```bash
# Start long polling (Terminal 1)
curl "http://localhost:8000/api/v1/poll?last_update_id=0&timeout=30"

# Trigger update (Terminal 2) 
curl -X POST "http://localhost:8000/api/v1/poll/trigger?update_type=test&message=Hello!"

# View statistics
curl http://localhost:8000/api/v1/poll/stats
```

### Server-Sent Events Test Commands:

```bash
# Connect to SSE stream (Terminal 1)
curl -N -H "Accept: text/event-stream" http://localhost:8000/api/v1/sse/stream

# Send broadcast message (Terminal 2)
curl -X POST "http://localhost:8000/api/v1/sse/broadcast?message=Hello%20SSE&event_type=test"

# Trigger demo update
curl -X POST "http://localhost:8000/api/v1/sse/trigger?update_type=demo&message=Test%20update"

# Get SSE statistics
curl http://localhost:8000/api/v1/sse/stats
```

## 🏗 Architecture

### Long Polling Flow:
```
Client Request ──► Server holds connection ──► Response when data available
     │                                              │
     └──────── Reduced HTTP requests ──────────────┘
```

### Server-Sent Events Flow:
```
Client ──► EventSource ──► Persistent HTTP stream ──► Real-time events
   │                                                       │
   └──────── Single connection, multiple events ──────────┘
```

## 🔧 Configuration

- **Timeout**: 1-60 seconds (default: 30)
- **Background Updates**: Every 5-15 seconds
- **Update History**: Last 50 updates kept
- **CORS**: Enabled for browser testing

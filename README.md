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

**MQTT (Message Queuing Telemetry Transport):**
- Publish-subscribe messaging pattern
- Quality of Service levels (0, 1, 2)
- Topic-based message routing
- Cross-system integration with Long Polling and SSE
- Message retention and persistent sessions

### 🚧 Coming Next
- WebSocket implementation
- Socket.IO integration

## 🎯 API Testing

**See [LONG_POLLING_DEMO.md](LONG_POLLING_DEMO.md) for Long Polling demo**
**See [SSE_DEMO.md](SSE_DEMO.md) for Server-Sent Events demo**
**See [MQTT_DEMO.md](MQTT_DEMO.md) for MQTT messaging demo**

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

### MQTT Test Commands:

```bash
# Publish message to topic
curl -X POST 'http://localhost:8000/api/v1/mqtt/publish?topic=demo/test&message=Hello%20MQTT&qos=1'

# Send demo notification
curl -X POST 'http://localhost:8000/api/v1/mqtt/demo?demo_type=notification&message=MQTT%20working'

# Get MQTT statistics
curl http://localhost:8000/api/v1/mqtt/stats

# View recent messages
curl 'http://localhost:8000/api/v1/mqtt/messages?limit=5'

# Subscribe to new topic
curl -X POST 'http://localhost:8000/api/v1/mqtt/subscribe?topic=custom/topic&qos=1'
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

### MQTT Flow:
```
Publisher ──► MQTT Broker ──► Subscribers
    │              │              │
    └── Topics ──── Routing ──── Delivery
```

### Cross-System Integration:
```
Background Updates ──► Long Polling ──► Waiting Clients
                  │
                  ├──► SSE ──────────► Connected Streams  
                  │
                  └──► MQTT ─────────► Topic Subscribers
```

## 🔧 Configuration

- **Long Polling**: 1-60 seconds timeout (default: 30)
- **SSE**: Automatic reconnection, heartbeat every 30 seconds
- **MQTT**: QoS levels 0-2, message retention, persistent sessions
- **Background Updates**: Every 5-15 seconds across all systems
- **Update History**: Last 50-100 messages kept
- **CORS**: Enabled for browser testing

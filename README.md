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

### ✅ Implemented: Long Polling
- Efficient real-time communication with 83% fewer HTTP requests
- Configurable timeout (1-60 seconds)
- Background data simulation
- Manual update triggers  
- Statistics and monitoring
- HTML demo client

### 🚧 Coming Next
- Server-Sent Events (SSE)
- MQTT integration
- WebSocket implementation
- Socket.IO integration

## 🎯 Demo Long Polling

**See [LONG_POLLING_DEMO.md](LONG_POLLING_DEMO.md) for step-by-step demo guide**

### Quick Test Commands:

```bash
# Check application
curl http://localhost:8000

# Start long polling (Terminal 1)
curl "http://localhost:8000/api/v1/poll?last_update_id=0&timeout=30"

# Trigger update (Terminal 2) 
curl -X POST "http://localhost:8000/api/v1/poll/trigger?update_type=test&message=Hello!"

# View statistics
curl http://localhost:8000/api/v1/poll/stats
```

### HTML Demo:
Open `long_polling_demo.html` in your browser for interactive testing.

## 🏗 Architecture

```
Client Request ──► Server holds connection ──► Response when data available
     │                                              │
     └──────── Reduced HTTP requests ──────────────┘
```

## 🔧 Configuration

- **Timeout**: 1-60 seconds (default: 30)
- **Background Updates**: Every 5-15 seconds
- **Update History**: Last 50 updates kept
- **CORS**: Enabled for browser testing

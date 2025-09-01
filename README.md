# Communication Techniques Demo

A comprehensive FastAPI application demonstrating various real-time communication techniques including Long Polling, Server-Sent Events (SSE), MQTT, WebSocket, and Socket.IO.

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Git

### Running the Application

1. **Clone and navigate to the project:**
   ```bash
   git clone <repository-url>
   cd HTTP_vs_LongPull_vs_Websocket_vs_socketIo
   ```

2. **Start the application with Docker Compose:**
   ```bash
   docker-compose up --build
   ```

3. **Access the application:**
   - FastAPI App: http://localhost:8000
   - API Documentation: http://localhost:8000/docs
   - MQTT Broker: localhost:1883

### Development Mode

For local development without Docker:
```bash
pip install -r requirements.txt
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

## 📋 Project Status

### ✅ Completed Features
- [x] **Phase 1**: Project setup + Dockerization
  - FastAPI base application
  - Docker containerization
  - MQTT broker setup
  - Development environment

### 🚧 Upcoming Features
- [ ] **Phase 2**: Long Polling implementation
- [ ] **Phase 3**: Server-Sent Events (SSE)
- [ ] **Phase 4**: MQTT integration
- [ ] **Phase 5**: WebSocket implementation
- [ ] **Phase 6**: Socket.IO integration

## 🛠 Architecture

```
┌─────────────────┐    ┌─────────────────┐
│   FastAPI App   │    │   MQTT Broker   │
│   (Port 8000)   │    │   (Port 1883)   │
└─────────────────┘    └─────────────────┘
         │                       │
         └───────────────────────┘
              Docker Network
```

## 📚 API Documentation

### Base Endpoints

#### Root Information
- **Method**: GET
- **Endpoint**: `/`
- **Objective**: Get application information and available techniques
- **Response**:
```json
{
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
```

#### Health Check
- **Method**: GET
- **Endpoint**: `/health`
- **Objective**: Verify application health status
- **Response**:
```json
{
  "status": "healthy",
  "timestamp": "2025-09-01T10:00:00Z"
}
```

## 🧪 Testing

Test the base setup:
```bash
# Test root endpoint
curl http://localhost:8000/

# Test health check
curl http://localhost:8000/health

# Access interactive API docs
open http://localhost:8000/docs
```

## 🔧 Configuration

### Environment Variables
- `MQTT_BROKER_HOST`: MQTT broker hostname (default: mqtt-broker)
- `MQTT_BROKER_PORT`: MQTT broker port (default: 1883)

### MQTT Configuration
- **Broker**: Eclipse Mosquitto
- **MQTT Port**: 1883
- **WebSocket Port**: 9001
- **Anonymous Access**: Enabled (for demo purposes)

## 📝 Development Notes

- Exception handling implemented across all endpoints
- CORS enabled for development
- Structured logging configured
- Mock data used (no database required)
- Docker health checks included

---

**Next Phase**: Long Polling implementation will be added with dedicated endpoints and real-time data simulation.

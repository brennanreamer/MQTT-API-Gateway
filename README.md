# MQTT API Gateway

A lightweight REST API gateway that subscribes to MQTT topics and exposes the cached data via HTTP endpoints. Can be used with any HTTP client.

## Features

- **Pre-configured topic subscriptions** - Automatically subscribes to specified MQTT topic patterns on startup
- **24-hour historical data** - Stores up to 10,000 messages per topic with automatic cleanup
- **Memory-safe design** - Uses bounded storage to prevent memory leaks
- **Basic HTTP Authentication** - Secures all API endpoints
- **Auto-generated API documentation** - Swagger UI available at `/docs`

## Configurable Topics

Topics are configured via the `MQTT_TOPICS` environment variable as a comma-separated list:

```env
MQTT_TOPICS=sensors/#,factory/line1/#,factory/line2/#
```

Wildcard patterns supported:
- `#` - Multi-level wildcard (all subtopics)
- `+` - Single-level wildcard

If not specified, defaults to `#` (subscribe to all topics).

## Quick Start

### 1. Install Dependencies

```bash
cd mqtt-api-gateway
pip install -r requirements.txt
```

### 2. Configure Environment

Rename `env.example.txt` to `.env` and update with your settings:

```env
MQTT_BROKER_HOST=your-broker.cloud.com
MQTT_BROKER_PORT=8883
MQTT_USERNAME=your_mqtt_username
MQTT_PASSWORD=your_mqtt_password
MQTT_USE_TLS=true
MQTT_TOPICS="sensors/#,factory/#"

API_USERNAME=api_user
API_PASSWORD=your_secure_api_password
```

### 3. Run the Server

```bash
python main.py
```

Or with uvicorn directly:

```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

The server will start at `http://localhost:8000`

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check (no auth required) |
| GET | `/topics` | List all topics with cached data |
| GET | `/topics/{topic}/latest` | Get most recent message for a topic |
| GET | `/topics/{topic}/history` | Get historical messages (last 24h) |

### Authentication

All endpoints except `/health` require HTTP Basic Authentication.

### Example: Get Latest Message

```bash
curl -u api_user:your_password \
  "http://localhost:8000/topics/sensors/temperature/latest"
```

Response:
```json
{
  "topic": "sensors/temperature",
  "timestamp": "2026-01-26T10:30:00Z",
  "payload": "72.5"
}
```

### Example: Get Message History

```bash
curl -u api_user:your_password \
  "http://localhost:8000/topics/sensors/temperature/history?limit=50"
```

Response:
```json
{
  "topic": "sensors/temperature",
  "message_count": 50,
  "messages": [
    {"timestamp": "2026-01-26T10:30:00Z", "payload": "72.5"},
    {"timestamp": "2026-01-26T10:29:00Z", "payload": "72.3"}
  ]
}
```

## API Documentation

Once running, visit:
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

## Deployment

### Windows Server

1. Install Python 3.10+
2. Clone/copy the project files
3. Install dependencies: `pip install -r requirements.txt`
4. Create `.env` file with your configuration
5. Run: `python main.py`

For production, consider using a process manager like NSSM to run as a Windows service.

### AWS EC2

1. Launch an EC2 instance (t3.micro is sufficient for most use cases)
2. Install Python 3.10+
3. Clone/copy the project files
4. Install dependencies
5. Configure security group to allow inbound traffic on port 8000
6. Use systemd or supervisor to manage the process

For HTTPS, put behind an Application Load Balancer with an SSL certificate.

## Memory Management

The gateway uses several mechanisms to prevent memory leaks:

1. **Bounded storage per topic** - Each topic can store up to `MAX_MESSAGES_PER_TOPIC` messages (default: 10,000)
2. **Time-based cleanup** - Messages older than `HISTORY_RETENTION_HOURS` (default: 24) are automatically removed
3. **Deque with maxlen** - Uses Python's `collections.deque` which automatically evicts oldest items

## Customizing Topics

Topics are configured via the `MQTT_TOPICS` environment variable. No code changes needed.

**Important:** Wrap values in quotes to allow `#` wildcard characters:

```env
# Single topic tree
MQTT_TOPICS="sensors/#"

# Multiple topic trees (comma-separated)
MQTT_TOPICS="factory/line1/#,factory/line2/#,warehouse/#"

# Subscribe to everything
MQTT_TOPICS="#"
```

## Troubleshooting

### Cannot connect to MQTT broker

- Verify `MQTT_BROKER_HOST` and `MQTT_BROKER_PORT` are correct
- Check that `MQTT_USE_TLS` matches your broker configuration
- Ensure your MQTT credentials are valid
- Check firewall rules allow outbound connections to the broker

### No messages appearing

- Verify the topic patterns match your MQTT data structure
- Check the `/health` endpoint to confirm MQTT connection status
- Review application logs for any error messages

### Memory usage growing

- Reduce `MAX_MESSAGES_PER_TOPIC` if storing too many messages
- Reduce `HISTORY_RETENTION_HOURS` for shorter retention
- Ensure the cleanup task is running (check logs)

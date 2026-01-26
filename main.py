"""
MQTT API Gateway
================
A FastAPI-based REST API gateway that caches MQTT messages and exposes them via HTTP endpoints.
"""

import os
import ssl
import json
import secrets
import logging
from datetime import datetime, timedelta, timezone
from collections import deque
from threading import Lock
from typing import Optional, Any
from contextlib import asynccontextmanager

import paho.mqtt.client as mqtt
from fastapi import FastAPI, HTTPException, Depends, Query, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel
from pydantic_settings import BaseSettings
from dotenv import load_dotenv
import asyncio

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("mqtt-gateway")


# =============================================================================
# Configuration
# =============================================================================

class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # MQTT Broker Settings
    mqtt_broker_host: str = "localhost"
    mqtt_broker_port: int = 8883
    mqtt_username: str = ""
    mqtt_password: str = ""
    mqtt_use_tls: bool = True
    
    # MQTT Topics (comma-separated list)
    mqtt_topics: str = "#"
    
    # API Authentication
    api_username: str = "admin"
    api_password: str = "changeme"
    
    # Memory Management
    max_messages_per_topic: int = 10000
    history_retention_hours: int = 24
    cleanup_interval_minutes: int = 5
    
    class Config:
        env_file = ".env"
        case_sensitive = False
    
    def get_topics_list(self) -> list[str]:
        """Parse comma-separated topics into a list."""
        if not self.mqtt_topics:
            return ["#"]  # Subscribe to all by default
        # Strip quotes in case they weren't removed by the env parser
        topics_str = self.mqtt_topics.strip('"\'')
        return [topic.strip() for topic in topics_str.split(",") if topic.strip()]


settings = Settings()


# =============================================================================
# Message Storage (Memory-Safe)
# =============================================================================

class MQTTMessage(BaseModel):
    """Represents a single MQTT message."""
    topic: str
    payload: Any
    timestamp: datetime


class MessageStore:
    """
    Thread-safe, memory-bounded storage for MQTT messages.
    
    Uses deque with maxlen to automatically evict oldest messages when
    the limit is reached, preventing memory leaks.
    """
    
    def __init__(self, max_messages_per_topic: int, retention_hours: int):
        self._store: dict[str, deque] = {}
        self._lock = Lock()
        self._max_messages = max_messages_per_topic
        self._retention_hours = retention_hours
    
    def add_message(self, topic: str, payload: Any) -> None:
        """Add a message to the store."""
        with self._lock:
            if topic not in self._store:
                self._store[topic] = deque(maxlen=self._max_messages)
            
            message = {
                "payload": payload,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            self._store[topic].append(message)
    
    def get_latest(self, topic: str) -> Optional[dict]:
        """Get the most recent message for a topic."""
        with self._lock:
            if topic not in self._store or len(self._store[topic]) == 0:
                return None
            return self._store[topic][-1]
    
    def get_history(self, topic: str, limit: Optional[int] = None) -> list[dict]:
        """Get historical messages for a topic, newest first."""
        with self._lock:
            if topic not in self._store:
                return []
            
            messages = list(self._store[topic])
            messages.reverse()  # Newest first
            
            if limit:
                messages = messages[:limit]
            
            return messages
    
    def get_all_topics(self) -> list[dict]:
        """Get a list of all topics with message counts."""
        with self._lock:
            return [
                {
                    "topic": topic,
                    "message_count": len(messages),
                    "latest_timestamp": messages[-1]["timestamp"] if messages else None
                }
                for topic, messages in self._store.items()
            ]
    
    def cleanup_old_messages(self) -> int:
        """
        Remove messages older than retention period.
        Returns the number of messages removed.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(hours=self._retention_hours)
        removed_count = 0
        
        with self._lock:
            for topic in self._store:
                original_len = len(self._store[topic])
                
                # Filter out old messages
                self._store[topic] = deque(
                    (msg for msg in self._store[topic] 
                     if datetime.fromisoformat(msg["timestamp"]) > cutoff),
                    maxlen=self._max_messages
                )
                
                removed_count += original_len - len(self._store[topic])
        
        return removed_count
    
    def get_stats(self) -> dict:
        """Get storage statistics."""
        with self._lock:
            total_messages = sum(len(msgs) for msgs in self._store.values())
            return {
                "total_topics": len(self._store),
                "total_messages": total_messages,
                "max_messages_per_topic": self._max_messages,
                "retention_hours": self._retention_hours
            }


# Initialize message store
message_store = MessageStore(
    max_messages_per_topic=settings.max_messages_per_topic,
    retention_hours=settings.history_retention_hours
)


# =============================================================================
# MQTT Client
# =============================================================================

mqtt_client: Optional[mqtt.Client] = None


def on_connect(client, userdata, flags, rc, properties=None):
    """Callback when connected to MQTT broker."""
    if rc == 0:
        logger.info("Connected to MQTT broker successfully")
        
        # Subscribe to all configured topics
        for topic in settings.get_topics_list():
            client.subscribe(topic)
            logger.info(f"Subscribed to: {topic}")
    else:
        logger.error(f"Failed to connect to MQTT broker. Return code: {rc}")


def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
    """Callback when disconnected from MQTT broker (paho-mqtt 2.x VERSION2 API)."""
    logger.warning(f"Disconnected from MQTT broker. Reason: {reason_code}")
    if reason_code != 0:
        logger.info("Unexpected disconnection. Will attempt to reconnect...")


_message_count = 0

def on_message(client, userdata, msg):
    """Callback when a message is received."""
    global _message_count
    try:
        # Try to decode payload as JSON, fallback to string
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            payload = msg.payload.decode("utf-8", errors="replace")
        
        message_store.add_message(msg.topic, payload)
        _message_count += 1
        
        # Log every 100 messages to avoid log spam but track activity
        if _message_count % 100 == 0:
            logger.info(f"DEBUG: Received {_message_count} total messages")
        
    except Exception as e:
        logger.error(f"Error processing message: {e}")


def create_mqtt_client() -> mqtt.Client:
    """Create and configure the MQTT client."""
    # Use MQTT v5 protocol
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        protocol=mqtt.MQTTv5
    )
    
    # Set callbacks
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    
    # Set authentication
    if settings.mqtt_username and settings.mqtt_password:
        client.username_pw_set(settings.mqtt_username, settings.mqtt_password)
    
    # Configure TLS if enabled
    if settings.mqtt_use_tls:
        client.tls_set(
            cert_reqs=ssl.CERT_REQUIRED,
            tls_version=ssl.PROTOCOL_TLS
        )
    
    return client


def start_mqtt_client():
    """Start the MQTT client connection."""
    global mqtt_client
    
    mqtt_client = create_mqtt_client()
    
    try:
        logger.info(f"Connecting to MQTT broker at {settings.mqtt_broker_host}:{settings.mqtt_broker_port}")
        mqtt_client.connect(
            settings.mqtt_broker_host,
            settings.mqtt_broker_port,
            keepalive=60
        )
        mqtt_client.loop_start()
        logger.info("MQTT client loop started")
    except Exception as e:
        logger.error(f"Failed to connect to MQTT broker: {e}")
        logger.warning("App will continue running - MQTT will retry automatically")
        # Don't raise - let the app start anyway, MQTT client will retry


def stop_mqtt_client():
    """Stop the MQTT client connection."""
    global mqtt_client
    
    if mqtt_client:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        logger.info("MQTT client disconnected")


# =============================================================================
# Background Tasks
# =============================================================================

async def cleanup_task():
    """Periodically clean up old messages."""
    while True:
        await asyncio.sleep(settings.cleanup_interval_minutes * 60)
        try:
            removed = message_store.cleanup_old_messages()
            if removed > 0:
                logger.info(f"Cleanup: removed {removed} old messages")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")


# =============================================================================
# FastAPI Application
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    # Startup
    logger.info("Starting MQTT API Gateway...")
    logger.info(f"DEBUG: PORT env = {os.environ.get('PORT', 'NOT SET')}")
    logger.info(f"DEBUG: About to start MQTT client...")
    start_mqtt_client()
    logger.info(f"DEBUG: MQTT client started, creating cleanup task...")
    
    # Start background cleanup task
    cleanup_task_handle = asyncio.create_task(cleanup_task())
    logger.info(f"DEBUG: Cleanup task created, yielding to app...")
    
    yield
    
    logger.info(f"DEBUG: App shutting down...")
    # Shutdown
    logger.info("Shutting down MQTT API Gateway...")
    cleanup_task_handle.cancel()
    stop_mqtt_client()


app = FastAPI(
    title="MQTT API Gateway",
    description="REST API gateway for MQTT data",
    version="1.0.0",
    lifespan=lifespan
)


# =============================================================================
# Authentication
# =============================================================================

security = HTTPBasic()


def verify_credentials(credentials: HTTPBasicCredentials = Depends(security)) -> str:
    """Verify HTTP Basic Authentication credentials."""
    correct_username = secrets.compare_digest(
        credentials.username.encode("utf-8"),
        settings.api_username.encode("utf-8")
    )
    correct_password = secrets.compare_digest(
        credentials.password.encode("utf-8"),
        settings.api_password.encode("utf-8")
    )
    
    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
    
    return credentials.username


# =============================================================================
# Response Models
# =============================================================================

class TopicInfo(BaseModel):
    """Information about a single topic."""
    topic: str
    message_count: int
    latest_timestamp: Optional[str] = None


class TopicsListResponse(BaseModel):
    """Response for listing all topics."""
    topics: list[TopicInfo]
    total_topics: int


class LatestMessageResponse(BaseModel):
    """Response for getting the latest message."""
    topic: str
    timestamp: str
    payload: Any


class HistoryResponse(BaseModel):
    """Response for getting message history."""
    topic: str
    message_count: int
    messages: list[dict]


class HealthResponse(BaseModel):
    """Response for health check."""
    status: str
    mqtt_connected: bool
    storage_stats: dict


# =============================================================================
# API Endpoints
# =============================================================================

@app.get("/", tags=["Root"])
async def root():
    """Simple root endpoint for basic connectivity test."""
    logger.info("DEBUG: / root endpoint called")
    return {"status": "ok", "message": "MQTT API Gateway is running"}


@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """
    Health check endpoint.
    
    Returns the current status of the gateway, including MQTT connection
    status and storage statistics.
    """
    logger.info("DEBUG: /health endpoint called")
    mqtt_connected = mqtt_client is not None and mqtt_client.is_connected()
    logger.info(f"DEBUG: mqtt_connected = {mqtt_connected}")
    
    return HealthResponse(
        status="healthy" if mqtt_connected else "degraded",
        mqtt_connected=mqtt_connected,
        storage_stats=message_store.get_stats()
    )


@app.get("/topics", response_model=TopicsListResponse, tags=["Topics"])
async def list_topics(username: str = Depends(verify_credentials)):
    """
    List all topics with cached data.
    
    Returns a list of all MQTT topics that have received messages,
    along with message counts and the timestamp of the most recent message.
    """
    topics = message_store.get_all_topics()
    
    return TopicsListResponse(
        topics=[TopicInfo(**t) for t in topics],
        total_topics=len(topics)
    )


@app.get("/topics/{topic:path}/latest", response_model=LatestMessageResponse, tags=["Topics"])
async def get_latest_message(
    topic: str,
    username: str = Depends(verify_credentials)
):
    """
    Get the most recent message for a topic.
    
    The topic path should be URL-encoded (spaces as %20).
    
    Example: /topics/sensors/temperature/latest
    """
    message = message_store.get_latest(topic)
    
    if message is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No messages found for topic: {topic}"
        )
    
    return LatestMessageResponse(
        topic=topic,
        timestamp=message["timestamp"],
        payload=message["payload"]
    )


@app.get("/topics/{topic:path}/history", response_model=HistoryResponse, tags=["Topics"])
async def get_message_history(
    topic: str,
    limit: Optional[int] = Query(default=100, ge=1, le=10000, description="Maximum number of messages to return"),
    username: str = Depends(verify_credentials)
):
    """
    Get historical messages for a topic.
    
    Returns messages in reverse chronological order (newest first).
    The topic path should be URL-encoded (spaces as %20).
    
    Example: /topics/sensors/temperature/history?limit=50
    """
    messages = message_store.get_history(topic, limit=limit)
    
    return HistoryResponse(
        topic=topic,
        message_count=len(messages),
        messages=messages
    )


# =============================================================================
# Main Entry Point
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    
    port = int(os.environ.get("PORT", 8000))
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        reload=False,
        workers=1  # Single worker to share MQTT connection state
    )

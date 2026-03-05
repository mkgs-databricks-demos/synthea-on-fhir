# app.py
import uuid
import json
import logging
from datetime import datetime, timezone
from typing import Optional
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request, HTTPException, Depends, Header, status
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties

from config import (
    ZEROBUS_SERVER_ENDPOINT,
    WORKSPACE_URL,
    FHIR_BUNDLE_TABLE_NAME,
    CLIENT_ID,
    CLIENT_SECRET,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Pydantic models for request/response validation
class IngestResponse(BaseModel):
    """Response model for successful FHIR bundle ingestion"""
    status: str = Field(default="ok", description="Status of the ingestion")
    bundle_uuid: str = Field(..., description="Unique identifier for the ingested bundle")
    user: str = Field(..., description="Authenticated user who submitted the bundle")
    timestamp: str = Field(..., description="ISO timestamp of ingestion")


class HealthResponse(BaseModel):
    """Response model for health check"""
    status: str = Field(..., description="Health status")
    zerobus_stream: str = Field(..., description="Zerobus stream status")
    timestamp: str = Field(..., description="ISO timestamp")


# Application state management using lifespan (replaces deprecated on_event)
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for startup and shutdown events.
    Best practice replacement for deprecated @app.on_event decorators.
    """
    # Startup: Initialize Zerobus stream
    logger.info("Starting FHIR Zerobus Ingest App...")
    try:
        # Create SDK client
        zerobus_sdk = ZerobusSdk(ZEROBUS_SERVER_ENDPOINT, WORKSPACE_URL)
        
        # Use JSON mode - protobuf mode requires C++ protobuf extensions not available in container
        logger.info(f"Initializing JSON stream for table: {FHIR_BUNDLE_TABLE_NAME}")
        table_props = TableProperties(FHIR_BUNDLE_TABLE_NAME)
        
        options = StreamConfigurationOptions(
            record_type=RecordType.JSON,  # Use JSON mode instead of PROTO
            max_inflight_records=10_000,
            recovery=True,
        )
        
        # Open a long-lived JSON stream for this table
        zerobus_stream = zerobus_sdk.create_stream(
            CLIENT_ID,
            CLIENT_SECRET,
            table_props,
            options,
        )
        
        # Store in app state (best practice for sharing across requests)
        app.state.zerobus_sdk = zerobus_sdk
        app.state.zerobus_stream = zerobus_stream
        
        logger.info(f"Successfully initialized Zerobus JSON stream for table: {FHIR_BUNDLE_TABLE_NAME}")
        
    except Exception as e:
        logger.error(f"Failed to initialize Zerobus stream: {e}", exc_info=True)
        app.state.zerobus_sdk = None
        app.state.zerobus_stream = None
    
    # Create async HTTP client for optional additional validation (connection pooling)
    app.state.http_client = httpx.AsyncClient(timeout=10.0)
    
    yield  # Application runs here
    
    # Shutdown: Clean up resources
    logger.info("Shutting down FHIR Zerobus Ingest App...")
    
    # Close Zerobus stream
    if hasattr(app.state, "zerobus_stream") and app.state.zerobus_stream is not None:
        try:
            app.state.zerobus_stream.close()
            logger.info("Zerobus stream closed successfully")
        except Exception as e:
            logger.error(f"Error closing Zerobus stream: {e}", exc_info=True)
    
    # Close HTTP client
    if hasattr(app.state, "http_client"):
        await app.state.http_client.aclose()
        logger.info("HTTP client closed")


# Initialize FastAPI app with lifespan
app = FastAPI(
    title="FHIR to Zerobus Ingest App",
    description="FastAPI application for ingesting FHIR bundles to Unity Catalog via Databricks Zerobus",
    version="1.0.0",
    lifespan=lifespan,
)


# Add CORS middleware for external API calls
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict to specific origins
    allow_credentials=True,
    allow_methods=["POST", "GET"],
    allow_headers=["*"],
)


# Dependency: Authentication middleware for Databricks Apps
async def verify_databricks_auth(request: Request) -> dict:
    """
    Validates Databricks authentication from headers forwarded by Databricks Apps Gateway.
    
    Databricks Apps Gateway validates Bearer tokens at the edge and forwards user identity
    via special headers instead of the original Authorization header:
    - x-forwarded-user: The authenticated user's identity (email)
    - x-forwarded-access-token: The user's access token (optional, for user authorization)
    
    This function trusts the gateway's validation and extracts user information from
    the forwarded headers. The gateway ensures only authenticated requests reach this app.
    
    Args:
        request: FastAPI request object containing forwarded headers
        
    Returns:
        dict: User information containing userName and optional access token
        
    Raises:
        HTTPException: If required headers are missing (app not accessed via Databricks Apps)
        
    References:
        - https://docs.databricks.com/dev-tools/databricks-apps/auth/
        - https://docs.databricks.com/dev-tools/databricks-apps/http-headers/
    """
    # Extract forwarded authentication headers
    user = request.headers.get("x-forwarded-user")
    access_token = request.headers.get("x-forwarded-access-token")
    
    # Validate that we have at least user identity
    if not user:
        logger.warning("Request received without x-forwarded-user header")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=(
                "Authentication required. This app must be accessed via Databricks Apps. "
                "The gateway validates Bearer tokens and forwards user identity via headers."
            ),
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    logger.info(f"Successfully authenticated user from forwarded headers: {user}")
    
    # Return user info in compatible format
    return {
        "userName": user,
        "accessToken": access_token,  # Optional: may be None
    }


# Health check endpoint (best practice for monitoring)
@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check(request: Request):
    """
    Health check endpoint for monitoring and load balancers.
    Returns the status of the application and Zerobus stream.
    """
    zerobus_status = "healthy" if (
        hasattr(request.app.state, "zerobus_stream") 
        and request.app.state.zerobus_stream is not None
    ) else "unavailable"
    
    return HealthResponse(
        status="healthy",
        zerobus_stream=zerobus_status,
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


# Root endpoint
@app.get("/", tags=["Info"])
async def root():
    """Root endpoint with API information"""
    return {
        "name": app.title,
        "version": app.version,
        "description": app.description,
        "endpoints": {
            "health": "/health",
            "docs": "/docs",
            "ingest": "/api/v1/ingest/fhir-bundle",
        },
    }


# Main ingestion endpoint
@app.post(
    "/api/v1/ingest/fhir-bundle",
    response_model=IngestResponse,
    status_code=status.HTTP_200_OK,
    tags=["Ingestion"],
    summary="Ingest FHIR Bundle",
    description="Accepts a FHIR Bundle (or any JSON) and streams it to Unity Catalog via Zerobus",
)
async def ingest_fhir_bundle(
    request: Request,
    user_info: dict = Depends(verify_databricks_auth),
):
    """
    Accepts arbitrary JSON (e.g., a FHIR Bundle) and writes it
    into the `fhir` VARIANT column via Zerobus streaming ingestion.
    
    Requires authentication via Databricks Apps Gateway.
    
    **Authentication:**
    - Access this endpoint via Databricks Apps with Bearer token
    - Gateway validates token and forwards user identity to this app
    - Include header: `Authorization: Bearer <your_databricks_token>`
    
    **For Programmatic Access:**
    - Use service principal with OAuth M2M credentials
    - See: https://docs.databricks.com/dev-tools/databricks-apps/connect-local/
    
    **Rate Limits:**
    - Recommended: < 1000 requests/minute per user
    
    **Response:**
    - Returns bundle UUID and authenticated user information
    """
    # Check Zerobus stream availability
    if not hasattr(request.app.state, "zerobus_stream") or request.app.state.zerobus_stream is None:
        logger.error("Zerobus stream not initialized")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Zerobus stream not initialized. Service temporarily unavailable.",
        )
    
    # Parse and validate JSON payload
    try:
        payload = await request.json()
    except Exception as e:
        logger.warning(f"Invalid JSON payload received: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid JSON payload: {str(e)}",
        )
    
    # Generate unique bundle ID and metadata
    bundle_uuid = str(uuid.uuid4())
    user_email = user_info.get("userName", "unknown")
    event_timestamp = datetime.now(timezone.utc)
    
    # Build JSON record matching table schema
    record = {
        "bundle_uuid": bundle_uuid,
        "fhir": payload,  # VARIANT column accepts nested JSON directly
        "source_system": "FHIR to Zerobus Ingest App",
        "event_timestamp": event_timestamp.isoformat(),  # ISO 8601 format for TIMESTAMP
        "user_email": user_email,
    }
    
    # Format timestamp for response (ISO 8601 with Z suffix)
    timestamp_str = event_timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    # Log record for debugging (excluding large payload)
    logger.info(f"Ingesting JSON record - UUID: {bundle_uuid}, User: {user_email}, Timestamp: {timestamp_str}")
    
    # Ingest to Zerobus with error handling
    try:
        zerobus_stream = request.app.state.zerobus_stream
        
        # Debug logging for stream type
        logger.debug(f"Stream type: {type(zerobus_stream).__name__}")
        
        # Serialize record to JSON string (Zerobus JSON mode expects string, not dict)
        record_json = json.dumps(record)
        
        # Ingest JSON record (returns offset immediately)
        offset = zerobus_stream.ingest_record_offset(record_json)
        
        logger.info(f"Successfully ingested bundle {bundle_uuid} for user {user_email} at offset {offset}")
        
    except Exception as e:
        logger.error(f"Failed to write to Zerobus: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Failed to write to Zerobus: {str(e)}",
        )
    
    # Return success response
    return IngestResponse(
        status="ok",
        bundle_uuid=bundle_uuid,
        user=user_email,
        timestamp=timestamp_str,
    )

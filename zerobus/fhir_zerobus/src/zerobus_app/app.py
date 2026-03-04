# app.py
import uuid
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
        
        table_props = TableProperties(FHIR_BUNDLE_TABLE_NAME)
        options = StreamConfigurationOptions(record_type=RecordType.JSON)
        
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
        
        logger.info(f"Successfully initialized Zerobus stream for table: {FHIR_BUNDLE_TABLE_NAME}")
        
    except Exception as e:
        logger.error(f"Failed to initialize Zerobus stream: {e}", exc_info=True)
        app.state.zerobus_sdk = None
        app.state.zerobus_stream = None
    
    # Create async HTTP client for token validation (connection pooling)
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
    title="FHIR → Zerobus Ingest App",
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


# Dependency: Authentication middleware for external API calls
async def verify_databricks_token(
    request: Request,
    authorization: Optional[str] = Header(None),
) -> dict:
    """
    Validates Databricks workspace token by calling the Databricks SCIM API.
    Uses async HTTP client with connection pooling for better performance.
    
    Args:
        request: FastAPI request object (for accessing app state)
        authorization: Authorization header with Bearer token
        
    Returns:
        dict: User information from SCIM API
        
    Raises:
        HTTPException: If token is missing, invalid, or expired
    """
    if not authorization:
        logger.warning("Request received without Authorization header")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization header missing. Include 'Authorization: Bearer <token>' header.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    if not authorization.startswith("Bearer "):
        logger.warning(f"Invalid authorization format: {authorization[:20]}...")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authorization format. Use 'Bearer <token>'.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    token = authorization.replace("Bearer ", "")
    
    # Verify token by calling Databricks SCIM API (async with connection pooling)
    try:
        http_client: httpx.AsyncClient = request.app.state.http_client
        
        response = await http_client.get(
            f"{WORKSPACE_URL}/api/2.0/preview/scim/v2/Me",
            headers={"Authorization": f"Bearer {token}"},
        )
        
        if response.status_code != 200:
            logger.warning(f"Token validation failed with status {response.status_code}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired token. Please provide a valid Databricks workspace token.",
                headers={"WWW-Authenticate": "Bearer"},
            )
        
        user_info = response.json()
        logger.info(f"Successfully authenticated user: {user_info.get('userName', 'unknown')}")
        return user_info
        
    except httpx.RequestError as e:
        logger.error(f"Token validation request error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Token validation service unavailable: {str(e)}",
        )
    except HTTPException:
        raise  # Re-raise HTTPExceptions
    except Exception as e:
        logger.error(f"Unexpected error during token validation: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal error during authentication",
        )


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
    user_info: dict = Depends(verify_databricks_token),
):
    """
    Accepts arbitrary JSON (e.g., a FHIR Bundle) and writes it
    into the `fhir` VARIANT column via Zerobus streaming ingestion.
    
    Requires valid Databricks workspace authentication via Bearer token.
    
    **Authentication:**
    - Include header: `Authorization: Bearer <your_databricks_token>`
    
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
    
    # Generate unique bundle ID
    bundle_uuid = str(uuid.uuid4())
    timestamp = datetime.now(timezone.utc).isoformat()
    user_email = user_info.get("userName", "unknown")
    
    # Shape the record to match the table schema
    record = {
        "bundle_uuid": bundle_uuid,
        "fhir": payload,
        "source_system": app.title,
        "event_timestamp": timestamp,
        "user_email": user_email,
    }
    
    # Ingest to Zerobus with error handling
    try:
        zerobus_stream = request.app.state.zerobus_stream
        ack = zerobus_stream.ingest_record(record)
        ack.wait_for_ack()  # Wait for acknowledgment
        
        logger.info(f"Successfully ingested bundle {bundle_uuid} for user {user_email}")
        
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
        timestamp=timestamp,
    )

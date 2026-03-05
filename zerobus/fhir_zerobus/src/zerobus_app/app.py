# app.py
import uuid
import json
import logging
from datetime import datetime, timezone
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, HTTPException, Depends, status
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

# Constants
SOURCE_SYSTEM_NAME = "FHIR to Zerobus Ingest App"
MAX_INFLIGHT_RECORDS = 10_000


# Pydantic models for request/response validation
class IngestResponse(BaseModel):
    """Response model for successful FHIR bundle ingestion"""
    status: str = Field(default="ok", description="Status of the ingestion")
    bundle_uuid: str = Field(..., description="Unique identifier for the ingested bundle")
    timestamp: str = Field(..., description="ISO timestamp of ingestion")


class HealthResponse(BaseModel):
    """Response model for health check"""
    status: str = Field(..., description="Health status")
    zerobus_stream: str = Field(..., description="Zerobus stream status")
    timestamp: str = Field(..., description="ISO timestamp")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown events."""
    # Startup: Initialize Zerobus stream
    logger.info("Starting FHIR Zerobus Ingest App...")
    try:
        zerobus_sdk = ZerobusSdk(ZEROBUS_SERVER_ENDPOINT, WORKSPACE_URL)
        
        logger.info(f"Initializing JSON stream for table: {FHIR_BUNDLE_TABLE_NAME}")
        table_props = TableProperties(FHIR_BUNDLE_TABLE_NAME)
        
        options = StreamConfigurationOptions(
            record_type=RecordType.JSON,
            max_inflight_records=MAX_INFLIGHT_RECORDS,
            recovery=True,
        )
        
        logger.info(f"Stream configuration: record_type=JSON, max_inflight={MAX_INFLIGHT_RECORDS}, recovery=True")
        
        zerobus_stream = zerobus_sdk.create_stream(
            CLIENT_ID,
            CLIENT_SECRET,
            table_props,
            options,
        )
        
        app.state.zerobus_sdk = zerobus_sdk
        app.state.zerobus_stream = zerobus_stream
        
        logger.info(f"Successfully initialized Zerobus JSON stream for table: {FHIR_BUNDLE_TABLE_NAME}")
        
    except Exception as e:
        logger.error(f"Failed to initialize Zerobus stream: {e}", exc_info=True)
        app.state.zerobus_sdk = None
        app.state.zerobus_stream = None
    
    yield
    
    # Shutdown: Clean up resources
    logger.info("Shutting down FHIR Zerobus Ingest App...")
    
    if hasattr(app.state, "zerobus_stream") and app.state.zerobus_stream is not None:
        try:
            app.state.zerobus_stream.close()
            logger.info("Zerobus stream closed successfully")
        except Exception as e:
            logger.error(f"Error closing Zerobus stream: {e}", exc_info=True)


# Initialize FastAPI app
app = FastAPI(
    title="FHIR to Zerobus Ingest App",
    description="FastAPI application for ingesting FHIR bundles to Unity Catalog via Databricks Zerobus",
    version="1.0.0",
    lifespan=lifespan,
)


# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # TODO: In production, restrict to specific origins
    allow_credentials=True,
    allow_methods=["POST", "GET"],
    allow_headers=["*"],
)


async def verify_databricks_auth(request: Request) -> dict:
    """
    Validates Databricks authentication from headers forwarded by Databricks Apps Gateway.
    
    The gateway validates Bearer tokens and forwards user identity via special headers:
    - x-forwarded-user: The authenticated user's identity (email)
    - x-forwarded-access-token: The user's access token (optional)
    
    Args:
        request: FastAPI request object containing forwarded headers
        
    Returns:
        dict: User information containing userName and optional access token
        
    Raises:
        HTTPException: If required headers are missing
        
    References:
        - https://docs.databricks.com/dev-tools/databricks-apps/auth/
        - https://docs.databricks.com/dev-tools/databricks-apps/http-headers/
    """
    user = request.headers.get("x-forwarded-user")
    access_token = request.headers.get("x-forwarded-access-token")
    
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
    
    logger.info(f"Authenticated user: {user}")
    
    return {
        "userName": user,
        "accessToken": access_token,
    }


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
    - Returns bundle UUID and timestamp
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
        payload_bytes = await request.body()
        payload_text = payload_bytes.decode('utf-8')
        payload_obj = json.loads(payload_text)
        # VARIANT columns require JSON strings, not nested objects
        payload_json_str = json.dumps(payload_obj, separators=(',', ':'))
    except json.JSONDecodeError as e:
        logger.warning(f"Invalid JSON payload received: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid JSON payload: {str(e)}",
        )
    except Exception as e:
        logger.warning(f"Error reading request body: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error reading request body: {str(e)}",
        )
    
    # Generate unique bundle ID and timestamp
    bundle_uuid = str(uuid.uuid4())
    event_timestamp = datetime.now(timezone.utc)
    timestamp_epoch_micros = int(event_timestamp.timestamp() * 1_000_000)
    timestamp_str = event_timestamp.isoformat().replace('+00:00', 'Z')
    
    # Build record matching table schema
    record = {
        "bundle_uuid": bundle_uuid,
        "fhir": payload_json_str,
        "source_system": SOURCE_SYSTEM_NAME,
        "event_timestamp": timestamp_epoch_micros
    }
    
    logger.info(f"Ingesting bundle - UUID: {bundle_uuid}, User: {user_info.get('userName', 'unknown')}")
    
    # Ingest to Zerobus
    try:
        zerobus_stream = request.app.state.zerobus_stream
        
        # Validate record structure
        json.dumps(record)  # Ensure full record is serializable
        json.loads(record["fhir"])  # Ensure FHIR field is valid JSON
        
        offset = zerobus_stream.ingest_record_offset(record)
        zerobus_stream.flush()
        
        logger.info(f"Successfully ingested bundle {bundle_uuid} at offset {offset}")
        
    except Exception as e:
        logger.error(f"Failed to write to Zerobus: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Failed to write to Zerobus: {str(e)}",
        )
    
    return IngestResponse(
        status="ok",
        bundle_uuid=bundle_uuid,
        timestamp=timestamp_str,
    )

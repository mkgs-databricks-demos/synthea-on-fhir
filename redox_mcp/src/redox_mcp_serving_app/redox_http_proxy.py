# redox_http_proxy.py
import asyncio
import hashlib
import json
import logging
import os
import signal
import subprocess
import sys
import tempfile
import time
import traceback
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, Optional, List

from databricks.sdk import WorkspaceClient
from fastapi import FastAPI, HTTPException, Response, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings

# ============================================================================
# Logging Configuration
# ============================================================================
logging.basicConfig(
    level=logging.INFO
    , format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    , handlers=[logging.StreamHandler(sys.stderr)]
)
logger = logging.getLogger("redox-proxy")

# ============================================================================
# Configuration Management with Pydantic Settings
# ============================================================================
class Settings(BaseSettings):
    """Application configuration from environment variables"""
    redox_client_id: str = Field(..., env="REDOX_CLIENT_ID")
    redox_private_key: str = Field(..., env="REDOX_PRIVATE_KEY")
    redox_public_key_id: str = Field(..., env="REDOX_PUBLIC_KEY_ID")
    oauth_private_key: str = Field(..., env="OAUTH_PRIVATE_KEY")
    oauth_client_id: str = Field(..., env="OAUTH_CLIENT_ID")
    oauth_key_id: str = Field(..., env="OAUTH_KEY_ID")
    redox_binary_volume: str = Field(..., env="REDOX_BINARY_VOLUME")
    
    # Optional configuration
    request_timeout: float = Field(35.0, env="REQUEST_TIMEOUT")
    max_restart_attempts: int = Field(3, env="MAX_RESTART_ATTEMPTS")
    restart_delay: float = Field(2.0, env="RESTART_DELAY")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

# ============================================================================
# Error Models
# ============================================================================
class ErrorResponse(BaseModel):
    """Standardized error response"""
    error_code: str
    message: str
    details: Optional[Dict[str, Any]] = None

# ============================================================================
# Request/Response Models
# ============================================================================
class JsonRpcRequest(BaseModel):
    jsonrpc: str
    method: str
    id: Optional[int | str] = None
    params: Optional[Dict[str, Any]] = None

class HealthResponse(BaseModel):
    status: str
    mcp_process: str
    message: Optional[str] = None
    redox_api_endpoint: Optional[str] = None
    uptime_seconds: Optional[float] = None

class MetricsResponse(BaseModel):
    process_alive: bool
    process_pid: Optional[int] = None
    pending_requests: int
    total_requests: int
    total_errors: int
    uptime_seconds: float
    restart_count: int
    initialized: bool

# ============================================================================
# Global Exception Handler
# ============================================================================
def handle_exception(exc_type, exc_value, exc_traceback):
    """Global exception handler for uncaught exceptions"""
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger.error(
        f"UNCAUGHT EXCEPTION: {exc_type.__name__}: {exc_value}"
        , exc_info=(exc_type, exc_value, exc_traceback)
    )

sys.excepthook = handle_exception

# ============================================================================
# Binary Management
# ============================================================================
class BinaryManager:
    """Manages MCP binary download and caching"""
    
    def __init__(self, workspace_client: WorkspaceClient, volume_path: str):
        self.w = workspace_client
        self.volume_path = volume_path
        self.cache_dir = Path("/tmp/redox_mcp_cache")
        self.cache_dir.mkdir(exist_ok=True)
        self.binary_name = "redox-mcp-linux-x64"
    
    def get_cached_binary_path(self) -> Optional[str]:
        """Check if binary exists in cache"""
        cached_path = self.cache_dir / self.binary_name
        if cached_path.exists() and os.access(cached_path, os.X_OK):
            logger.info(f"Found cached binary at: {cached_path}")
            return str(cached_path)
        return None
    
    def download_and_cache_binary(self) -> str:
        """Download binary from volume and cache it"""
        cached_path = self.get_cached_binary_path()
        if cached_path:
            return cached_path
        
        volume_binary_path = f"{self.volume_path}/{self.binary_name}"
        logger.info(f"Downloading binary from volume: {volume_binary_path}")
        
        try:
            # Download to temporary file first
            temp_binary = tempfile.NamedTemporaryFile(
                mode='wb'
                , delete=False
                , suffix='-redox-mcp'
                , dir=str(self.cache_dir)
            )
            temp_binary_path = temp_binary.name
            
            logger.info("Initiating download...")
            response = self.w.files.download(volume_binary_path)
            logger.info("Download complete, writing to temp file...")
            temp_binary.write(response.contents.read())
            temp_binary.close()
            
            # Set executable permissions
            os.chmod(temp_binary_path, 0o755)
            logger.info(f"Binary downloaded to: {temp_binary_path}")
            
            # Test the binary
            self._test_binary(temp_binary_path)
            
            # Move to cache location
            final_path = self.cache_dir / self.binary_name
            Path(temp_binary_path).rename(final_path)
            logger.info(f"Binary cached at: {final_path}")
            
            return str(final_path)
            
        except Exception as e:
            logger.error(f"ERROR initializing binary: {e}", exc_info=True)
            raise
    
    def _test_binary(self, binary_path: str) -> None:
        """Test binary execution"""
        logger.info("Testing binary execution...")
        try:
            test_result = subprocess.run(
                [binary_path, "--version"]
                , capture_output=True
                , text=True
                , timeout=5
            )
            logger.info(f"Binary test - return code: {test_result.returncode}")
            logger.info(f"Binary test - stdout: {test_result.stdout}")
            if test_result.stderr:
                logger.info(f"Binary test - stderr: {test_result.stderr}")
        except Exception as test_e:
            logger.warning(f"Binary test failed: {test_e}")

# ============================================================================
# MCP Process Manager
# ============================================================================
class RedoxMCPProcess:
    """Manages the MCP subprocess with proper concurrency handling"""
    
    def __init__(self, binary_path: str, settings: Settings):
        self.binary_path = binary_path
        self.settings = settings
        self._cmd: List[str] = [binary_path]
        self._proc: Optional[subprocess.Popen] = None
        self._pending: Dict[Any, asyncio.Future] = {}
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._tools_cache: Optional[List[Dict[str, Any]]] = None
        self._request_lock = asyncio.Lock()  # Concurrency safety
        self._start_time: Optional[float] = None
        self._restart_count: int = 0
        self._total_requests: int = 0
        self._total_errors: int = 0
        self._read_task: Optional[asyncio.Task] = None
        self._stderr_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()
        self._initialized: bool = False
        self._init_response: Optional[Dict[str, Any]] = None
        logger.info(f"RedoxMCPProcess initialized with command: {self._cmd}")
    
    async def start(self) -> None:
        """Start the MCP process"""
        if self._proc is not None and self.is_alive():
            logger.info("MCP process already running, skipping start")
            return
        
        if self._loop is None:
            self._loop = asyncio.get_running_loop()
            logger.info("Event loop acquired")
        
        logger.info(f"Starting MCP process with command: {self._cmd}")
        
        # Copy current environment
        env = os.environ.copy()
        
        # Log OAuth-related env vars (sanitized)
        oauth_vars = [k for k in env.keys() if 
                     'CLIENT' in k.upper() or 
                     'KEY' in k.upper() or 
                     'OAUTH' in k.upper() or 
                     'REDOX' in k.upper()]
        for var in sorted(oauth_vars):
            value = env[var]
            if 'PATH' in var or 'VOLUME' in var:
                logger.info(f"  {var}: {value}")
            else:
                logger.info(f"  {var}: {value[:20]}... (length={len(value)})")
        
        try:
            self._proc = subprocess.Popen(
                self._cmd
                , stdin=subprocess.PIPE
                , stdout=subprocess.PIPE
                , stderr=subprocess.PIPE
                , bufsize=0
                , env=env
            )
            logger.info(f"MCP process started with PID: {self._proc.pid}")
            self._start_time = time.time()
            self._shutdown_event.clear()
            
            # Wait and check if process started successfully
            await asyncio.sleep(0.5)
            if self._proc.poll() is not None:
                exit_code = self._proc.returncode
                stderr_output = self._proc.stderr.read().decode('utf-8') if self._proc.stderr else "No stderr"
                stdout_output = self._proc.stdout.read().decode('utf-8') if self._proc.stdout else "No stdout"
                logger.error(f"MCP process exited immediately with code {exit_code}")
                logger.error(f"STDERR: {stderr_output}")
                logger.error(f"STDOUT: {stdout_output}")
                raise RuntimeError(f"MCP process failed to start (exit code {exit_code}): {stderr_output}")
                
        except Exception as e:
            logger.error(f"ERROR starting subprocess: {e}", exc_info=True)
            raise
        
        if self._proc.stdin is None or self._proc.stdout is None:
            raise RuntimeError("Failed to open pipes to redox-mcp")
        
        logger.info("MCP process running, starting read loops...")
        # Store task references for proper cleanup
        self._read_task = self._loop.create_task(self._read_loop())
        self._stderr_task = self._loop.create_task(self._stderr_loop())
        
        # Monitor process health
        self._loop.create_task(self._monitor_process())
    
    async def _monitor_process(self) -> None:
        """Monitor process health and log if it dies unexpectedly"""
        while not self._shutdown_event.is_set():
            await asyncio.sleep(1.0)
            if self._proc is not None and not self.is_alive():
                exit_code = self._proc.returncode
                logger.error(f"MCP process died unexpectedly with exit code: {exit_code}")
                # Try to read any remaining stderr
                try:
                    if self._proc.stderr:
                        remaining_stderr = self._proc.stderr.read().decode('utf-8')
                        if remaining_stderr:
                            logger.error(f"Remaining stderr: {remaining_stderr}")
                except Exception:
                    pass
                break
    
    async def ensure_alive(self) -> None:
        """Ensure process is alive, restart if necessary"""
        if not self.is_alive():
            if self._restart_count >= self.settings.max_restart_attempts:
                logger.error(f"Max restart attempts ({self.settings.max_restart_attempts}) reached")
                raise RuntimeError("MCP process repeatedly failing, max restart attempts exceeded")
            
            logger.warning(f"MCP process died, attempting restart (attempt {self._restart_count + 1})")
            await self.stop()
            await asyncio.sleep(self.settings.restart_delay)
            await self.start()
            self._restart_count += 1
    
    async def _stderr_loop(self) -> None:
        """Read stderr from MCP process"""
        if self._proc is None or self._proc.stderr is None:
            return
        
        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        
        try:
            await self._loop.connect_read_pipe(lambda: protocol, self._proc.stderr)
            
            while not self._shutdown_event.is_set():
                try:
                    line = await asyncio.wait_for(reader.readline(), timeout=1.0)
                    if not line:
                        logger.info("Stderr loop: EOF reached")
                        break
                    line = line.decode("utf-8").strip()
                    if line:
                        logger.info(f"[MCP STDERR] {line}")
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    if not self._shutdown_event.is_set():
                        logger.error(f"Error reading stderr: {e}", exc_info=True)
                    break
                    
        except Exception as e:
            logger.error(f"Error in stderr loop setup: {e}", exc_info=True)
        finally:
            logger.info("Stderr loop terminated")
    
    def _sanitize_json_response(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        """Sanitize JSON response to ensure it's serializable"""
        try:
            return json.loads(json.dumps(msg))
        except Exception as e:
            logger.warning(f"Failed to sanitize JSON: {e}")
            return msg
    
    async def _read_loop(self) -> None:
        """Read stdout from MCP process and route responses to pending futures"""
        if self._proc is None or self._proc.stdout is None:
            logger.error("Cannot start read loop: process or stdout is None")
            return
        
        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        
        try:
            await self._loop.connect_read_pipe(lambda: protocol, self._proc.stdout)
            logger.info("Read loop connected to process stdout")
            
            while not self._shutdown_event.is_set():
                try:
                    # Use timeout to allow checking shutdown event
                    line = await asyncio.wait_for(reader.readline(), timeout=1.0)
                    if not line:
                        logger.info("Read loop: EOF reached (process stdout closed)")
                        break
                    
                    line = line.decode("utf-8").strip()
                    if not line:
                        continue
                    
                    logger.debug(f"Raw line from MCP: {repr(line[:200])}")
                    
                    try:
                        msg = json.loads(line)
                        msg = self._sanitize_json_response(msg)
                        logger.debug(f"Parsed JSON message: {json.dumps(msg)[:200]}...")
                    except json.JSONDecodeError as je:
                        logger.error(f"JSON decode error: {je}")
                        logger.error(f"Problematic line: {repr(line)}")
                        continue
                    
                    rpc_id = msg.get("id")
                    if rpc_id is not None and rpc_id in self._pending:
                        fut = self._pending.pop(rpc_id)
                        if not fut.done():
                            fut.set_result(msg)
                    else:
                        logger.debug(f"Unmatched/notification message ID: {rpc_id}")
                        
                except asyncio.TimeoutError:
                    # Timeout is expected, continue to check shutdown event
                    continue
                except Exception as e:
                    if not self._shutdown_event.is_set():
                        logger.error(f"Error reading from stdout: {e}", exc_info=True)
                    break
                    
        except Exception as e:
            logger.error(f"Error in read loop setup: {e}", exc_info=True)
        finally:
            logger.info("Read loop terminated, cleaning up pending futures")
            # Clean up any pending futures
            for rpc_id, fut in list(self._pending.items()):
                if not fut.done():
                    fut.set_exception(RuntimeError("MCP process read loop terminated"))
            self._pending.clear()
    
    async def send(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Send request to MCP process with concurrency protection"""
        # Ensure process is alive before sending
        await self.ensure_alive()
        
        if self._proc is None or self._proc.stdin is None:
            raise HTTPException(status_code=500, detail="MCP process not running")
        
        # Use lock to serialize requests and prevent race conditions
        async with self._request_lock:
            self._total_requests += 1
            rpc_id = request.get("id")
            data = json.dumps(request) + "\n"
            
            logger.info(f"Sending request (method={request.get('method')}, id={rpc_id})")
            logger.debug(f"Request: {json.dumps(request, indent=2)}")
            logger.debug(f"Request length: {len(data)} bytes")
            
            try:
                self._proc.stdin.write(data.encode("utf-8"))
                self._proc.stdin.flush()
            except Exception as e:
                self._total_errors += 1
                logger.error(f"Failed to send request: {e}", exc_info=True)
                raise HTTPException(status_code=500, detail=f"Failed to send request to MCP: {str(e)}")
            
            # Check if process is still alive after sending
            if not self.is_alive():
                self._total_errors += 1
                logger.error("Process died after sending request")
                raise HTTPException(status_code=500, detail="MCP process died")
            
            if rpc_id is None:
                return {}
            
            fut: asyncio.Future = self._loop.create_future()
            self._pending[rpc_id] = fut
            
            try:
                # Use longer timeout for initialize (may need to auth with Redox API)
                method = request.get("method", "")
                if method == "initialize":
                    timeout = 90.0  # 90 seconds for initialize
                    logger.info(f"Using extended timeout ({timeout}s) for initialize request")
                else:
                    timeout = self.settings.request_timeout
                
                resp = await asyncio.wait_for(fut, timeout=timeout)
                logger.info(f"Received response for request ID: {rpc_id}")
                logger.debug(f"Response: {json.dumps(resp)[:200]}...")
                return resp
            except asyncio.TimeoutError:
                self._total_errors += 1
                self._pending.pop(rpc_id, None)
                method = request.get("method", "")
                logger.error(f"Timeout waiting for response to request ID: {rpc_id} (method={method})")
                if method == "initialize":
                    logger.error("Initialize timed out - MCP binary may be failing to authenticate with Redox API")
                raise HTTPException(
                    status_code=504
                    , detail=f"Timeout waiting for MCP response (method={method})"
                )
            except Exception as e:
                self._total_errors += 1
                self._pending.pop(rpc_id, None)
                logger.error(f"Error waiting for response: {e}", exc_info=True)
                raise
    
    def is_alive(self) -> bool:
        """Check if the MCP process is running"""
        return self._proc is not None and self._proc.poll() is None
    
    def get_uptime(self) -> float:
        """Get process uptime in seconds"""
        if self._start_time is None:
            return 0.0
        return time.time() - self._start_time
    
    def get_metrics(self) -> MetricsResponse:
        """Get process metrics"""
        return MetricsResponse(
            process_alive=self.is_alive()
            , process_pid=self._proc.pid if self._proc else None
            , pending_requests=len(self._pending)
            , total_requests=self._total_requests
            , total_errors=self._total_errors
            , uptime_seconds=self.get_uptime()
            , restart_count=self._restart_count
            , initialized=self._initialized
        )
    
    async def initialize_server(self) -> Dict[str, Any]:
        """Initialize the MCP server (called once during startup)"""
        if self._initialized and self._init_response is not None:
            logger.info("MCP server already initialized, returning cached response")
            return self._init_response
        
        logger.info("Initializing MCP server...")
        init_response = await self.send({
            "jsonrpc": "2.0"
            , "method": "initialize"
            , "id": "startup_init"
            , "params": {
                "protocolVersion": "2025-11-25"
                , "capabilities": {}
                , "clientInfo": {
                    "name": "redox-http-proxy"
                    , "version": "1.0.0"
                }
            }
        })
        
        if "error" in init_response:
            logger.error(f"MCP initialization failed: {init_response['error']}")
            raise RuntimeError(f"MCP initialization failed: {init_response['error']}")
        
        logger.info(f"MCP server initialized: {json.dumps(init_response)[:200]}")
        
        # Override the binary's protocolVersion to match what we sent
        # This allows the HTTP proxy to act as a protocol version adapter
        if "result" in init_response and "protocolVersion" in init_response["result"]:
            original_version = init_response["result"]["protocolVersion"]
            init_response["result"]["protocolVersion"] = "2025-11-25"
            logger.info(f"Protocol version override: {original_version} -> 2025-11-25")
        
        # Send initialized notification (required by MCP protocol)
        logger.info("Sending initialized notification...")
        await self.send({
            "jsonrpc": "2.0"
            , "method": "notifications/initialized"
            , "params": {}
        })
        
        self._initialized = True
        self._init_response = init_response
        logger.info("MCP server fully initialized and ready")
        return init_response
    
    async def list_tools(self) -> List[Dict[str, Any]]:
        """Query the MCP server for available tools"""
        if self._tools_cache is not None:
            logger.info("Returning cached tools list")
            return self._tools_cache
        
        logger.info("Fetching tools list from MCP server")
        response = await self.send({
            "jsonrpc": "2.0"
            , "method": "tools/list"
            , "id": "list_tools_req"
            , "params": {}
        })
        
        if "error" in response:
            logger.error(f"Error listing tools: {response['error']}")
            raise HTTPException(status_code=500, detail=f"MCP error: {response['error']}")
        
        tools = response.get("result", {}).get("tools", [])
        self._tools_cache = tools
        logger.info(f"Retrieved {len(tools)} tools from MCP server")
        return tools
    
    async def stop(self) -> None:
        """Stop the MCP process gracefully"""
        if self._proc is None:
            logger.info("No process to stop")
            return
        
        try:
            logger.info("Stopping MCP process...")
            self._shutdown_event.set()
            
            # Cancel tasks gracefully
            if self._read_task and not self._read_task.done():
                self._read_task.cancel()
                try:
                    await asyncio.wait_for(self._read_task, timeout=2.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
            
            if self._stderr_task and not self._stderr_task.done():
                self._stderr_task.cancel()
                try:
                    await asyncio.wait_for(self._stderr_task, timeout=2.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
            
            # Stop the process
            if self.is_alive():
                self._proc.send_signal(signal.SIGTERM)
                try:
                    self._proc.wait(timeout=5)
                    logger.info(f"Process terminated gracefully with code: {self._proc.returncode}")
                except subprocess.TimeoutExpired:
                    logger.warning("Process didn't stop gracefully, killing...")
                    self._proc.kill()
                    self._proc.wait(timeout=2)
        except Exception as e:
            logger.error(f"Error stopping process: {e}", exc_info=True)
        finally:
            self._proc = None
            self._read_task = None
            self._stderr_task = None
            logger.info("MCP process stopped")

# ============================================================================
# JSON-RPC Error Codes (per JSON-RPC 2.0 specification)
# ============================================================================
class JsonRpcErrorCode:
    """Standard JSON-RPC 2.0 error codes"""
    PARSE_ERROR = -32700
    INVALID_REQUEST = -32600
    METHOD_NOT_FOUND = -32601
    INVALID_PARAMS = -32602
    INTERNAL_ERROR = -32603
    SERVER_ERROR = -32000  # -32000 to -32099 reserved for implementation-defined errors

def create_jsonrpc_error_response(
    code: int
    , message: str
    , data: Optional[Dict[str, Any]] = None
    , request_id: Optional[int | str] = None
) -> Dict[str, Any]:
    """Create a JSON-RPC 2.0 compliant error response"""
    error_response = {
        "jsonrpc": "2.0"
        , "error": {
            "code": code
            , "message": message
        }
        , "id": request_id
    }
    
    if data is not None:
        error_response["error"]["data"] = data
    
    return error_response

# ============================================================================
# SSE (Server-Sent Events) Helper Functions
# ============================================================================
def format_sse_message(data: Dict[str, Any], event: Optional[str] = None) -> str:
    """Format a message as Server-Sent Events (SSE) format"""
    lines = []
    if event:
        lines.append(f"event: {event}")
    
    # SSE requires data to be on lines starting with "data: "
    json_data = json.dumps(data)
    lines.append(f"data: {json_data}")
    lines.append("")  # Empty line terminates the message
    
    return "\n".join(lines) + "\n"

async def sse_generator(response_data: Dict[str, Any]):
    """Generator for SSE responses"""
    # Send the response as SSE
    yield format_sse_message(response_data, event="message")
    
    # Send done event to indicate completion
    yield format_sse_message({"done": True}, event="done")

def should_use_sse(request: Request) -> bool:
    """Determine if the request expects SSE response based on Accept header"""
    accept_header = request.headers.get("accept", "")
    return "text/event-stream" in accept_header

# ============================================================================
# Application Initialization
# ============================================================================
logger.info("Starting application initialization...")

# Load settings
try:
    settings = Settings()
    logger.info("Configuration loaded successfully")
    logger.info(f"Request timeout: {settings.request_timeout}s")
    logger.info(f"Max restart attempts: {settings.max_restart_attempts}")
except Exception as e:
    logger.error(f"FATAL: Failed to load configuration: {e}", exc_info=True)
    raise

# Initialize workspace client and binary manager
w = WorkspaceClient()
binary_manager = BinaryManager(w, settings.redox_binary_volume)

# Download and cache binary
try:
    binary_path = binary_manager.download_and_cache_binary()
    logger.info(f"Binary ready at: {binary_path}")
except Exception as e:
    logger.error(f"FATAL: Binary initialization failed: {e}", exc_info=True)
    raise

# Create MCP process instance
try:
    redox_proc = RedoxMCPProcess(binary_path, settings)
    logger.info("RedoxMCPProcess instance created successfully")
except Exception as e:
    logger.error(f"FATAL: Failed to create RedoxMCPProcess: {e}", exc_info=True)
    raise

# ============================================================================
# FastAPI Application Setup
# ============================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    logger.info("FastAPI lifespan startup BEGIN")
    try:
        await redox_proc.start()
        logger.info("Redox MCP process started successfully")
        
        # Proactively initialize the MCP server during startup
        await redox_proc.initialize_server()
        logger.info("MCP server initialized and ready to handle requests")
        
    except Exception as e:
        logger.error(f"ERROR during startup: {e}", exc_info=True)
        raise
    
    logger.info("Lifespan startup complete, app is ready")
    yield
    
    logger.info("FastAPI lifespan shutdown BEGIN")
    await redox_proc.stop()
    logger.info("FastAPI lifespan shutdown COMPLETE")

# Create FastAPI app
app = FastAPI(
    title="Redox MCP HTTP Proxy"
    , description="HTTP proxy for Redox MCP stdio server"
    , version="1.0.0"
    , lifespan=lifespan
)

# Add CORS middleware for Databricks Apps
app.add_middleware(
    CORSMiddleware
    , allow_origins=["*"]  # Configure based on your security requirements
    , allow_credentials=True
    , allow_methods=["*"]
    , allow_headers=["*"]
)

# ============================================================================
# Exception Handlers
# ============================================================================
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Handle HTTP exceptions with structured error response"""
    return JSONResponse(
        status_code=exc.status_code
        , content=ErrorResponse(
            error_code=f"HTTP_{exc.status_code}"
            , message=exc.detail
        ).model_dump()
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle unexpected exceptions"""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500
        , content=ErrorResponse(
            error_code="INTERNAL_ERROR"
            , message="An unexpected error occurred"
            , details={"error": str(exc)}
        ).model_dump()
    )

# ============================================================================
# API Endpoints
# ============================================================================
@app.get("/")
async def root():
    """Root endpoint with basic info"""
    return {
        "service": "Redox MCP HTTP Proxy"
        , "status": "running"
        , "mcp_process_alive": redox_proc.is_alive()
        , "mcp_initialized": redox_proc._initialized
        , "uptime_seconds": redox_proc.get_uptime()
        , "endpoints": {
            "health": "/api/v1/health"
            , "metrics": "/api/v1/metrics"
            , "tools": "/api/v1/tools"
            , "debug_env": "/api/v1/debug/env"
            , "debug_process": "/api/v1/debug/process"
            , "debug_test_mcp": "/api/v1/debug/test-mcp"
            , "mcp": "/mcp (legacy)"
            , "messages": "/messages (MCP standard)"
            , "mcp_v1": "/mcp/v1 (versioned)"
        }
    }

@app.get("/api/v1/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint for monitoring"""
    is_alive = redox_proc.is_alive()
    is_initialized = redox_proc._initialized
    
    if not is_alive:
        return JSONResponse(
            status_code=503
            , content=HealthResponse(
                status="unhealthy"
                , mcp_process="stopped"
                , message="MCP process is not running"
            ).model_dump()
        )
    
    if not is_initialized:
        return JSONResponse(
            status_code=503
            , content=HealthResponse(
                status="unhealthy"
                , mcp_process="running"
                , message="MCP server not yet initialized"
            ).model_dump()
        )
    
    return HealthResponse(
        status="healthy"
        , mcp_process="running"
        , redox_api_endpoint="https://api.redoxengine.com/"
        , uptime_seconds=redox_proc.get_uptime()
    )

@app.get("/api/v1/metrics", response_model=MetricsResponse)
async def metrics():
    """Metrics endpoint for observability"""
    return redox_proc.get_metrics()

@app.get("/api/v1/debug/env")
async def debug_env():
    """Debug endpoint to show OAuth-related environment variables"""
    oauth_vars = {k: v for k, v in os.environ.items() if 
                  'CLIENT' in k.upper() or 
                  'KEY' in k.upper() or 
                  'OAUTH' in k.upper() or 
                  'REDOX' in k.upper()}
    
    # Sanitize sensitive values
    sanitized = {}
    for k, v in oauth_vars.items():
        if 'PATH' in k or 'VOLUME' in k:
            sanitized[k] = v  # Show paths in full
        else:
            sanitized[k] = f"{v[:10]}...{v[-10:]} (length={len(v)})"  # Show partial for secrets
    
    return {
        "environment_variables": sanitized
        , "count": len(oauth_vars)
        , "note": "Sensitive values are partially masked"
    }

@app.get("/api/v1/debug/process")
async def debug_process():
    """Debug endpoint to check MCP process status"""
    return {
        "process_alive": redox_proc.is_alive()
        , "process_pid": redox_proc._proc.pid if redox_proc._proc else None
        , "initialized": redox_proc._initialized
        , "pending_requests": len(redox_proc._pending)
        , "uptime_seconds": redox_proc.get_uptime()
        , "restart_count": redox_proc._restart_count
        , "binary_path": redox_proc.binary_path
        , "total_requests": redox_proc._total_requests
        , "total_errors": redox_proc._total_errors
    }

@app.post("/api/v1/debug/test-mcp")
async def test_mcp():
    """Debug endpoint to test basic MCP communication with a simple ping"""
    logger.info("Testing MCP communication with ping request")
    try:
        # Try a simple request to see if we get any response
        resp = await redox_proc.send({
            "jsonrpc": "2.0"
            , "method": "ping"
            , "id": "test_ping"
            , "params": {}
        })
        return {
            "success": True
            , "message": "MCP binary responded"
            , "response": resp
        }
    except Exception as e:
        logger.error(f"MCP test failed: {e}", exc_info=True)
        return {
            "success": False
            , "error": str(e)
            , "message": "MCP binary did not respond"
        }

@app.get("/api/v1/tools")
async def list_tools():
    """List all available tools from the MCP server"""
    try:
        await redox_proc.ensure_alive()
        tools = await redox_proc.list_tools()
        return {
            "tools": tools
            , "count": len(tools)
            , "redox_api_connected": True
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error listing tools: {e}", exc_info=True)
        raise HTTPException(
            status_code=500
            , detail=f"Failed to list tools. This may indicate authentication issues with Redox API: {str(e)}"
        )

@app.post("/mcp")
@app.post("/messages")
@app.post("/mcp/v1")
async def mcp_endpoint(req: JsonRpcRequest, request: Request):
    """Main MCP endpoint for JSON-RPC requests with SSE support
    
    Supports multiple paths for compatibility:
    - /mcp: Legacy path for backward compatibility
    - /messages: Standard MCP streamable-http path
    - /mcp/v1: Versioned API path
    """
    logger.info(f"MCP endpoint called with method: {req.method}")
    
    # Check if client disconnected
    if await request.is_disconnected():
        logger.warning("Client disconnected before processing request")
        error_response = create_jsonrpc_error_response(
            code=JsonRpcErrorCode.SERVER_ERROR
            , message="Client disconnected"
            , request_id=req.id
        )
        return JSONResponse(status_code=499, content=error_response)
    
    # Handle initialize specially - return cached response if already initialized
    if req.method == "initialize" and redox_proc._initialized and redox_proc._init_response:
        logger.info("Returning cached initialize response (server already initialized)")
        cached_response = redox_proc._init_response
        
        # Check if SSE is requested
        if should_use_sse(request):
            return StreamingResponse(
                sse_generator(cached_response)
                , media_type="text/event-stream"
                , headers={
                    "Cache-Control": "no-cache"
                    , "Connection": "keep-alive"
                    , "X-Accel-Buffering": "no"
                }
            )
        return cached_response
    
    request_dict = req.model_dump(exclude_none=True)
    
    try:
        resp = await redox_proc.send(request_dict)
        logger.info(f"Returning response for method: {req.method}")
        
        # Check for errors in the response
        if "error" in resp:
            error_detail = resp["error"]
            logger.error(f"MCP returned error: {error_detail}")
            
            # Check if it's an authentication error
            error_message = error_detail.get("message", "")
            if "auth" in error_message.lower() or "unauthorized" in error_message.lower():
                logger.error("AUTHENTICATION ERROR detected with Redox API")
        
        # Return as SSE if requested
        if should_use_sse(request):
            return StreamingResponse(
                sse_generator(resp)
                , media_type="text/event-stream"
                , headers={
                    "Cache-Control": "no-cache"
                    , "Connection": "keep-alive"
                    , "X-Accel-Buffering": "no"
                }
            )
        
        return resp
    except HTTPException as http_exc:
        # Convert HTTPException to JSON-RPC error for MCP compliance
        logger.error(f"HTTP error in mcp_endpoint: {http_exc.detail}")
        
        # Map HTTP status to JSON-RPC error code
        if http_exc.status_code == 504:
            code = JsonRpcErrorCode.INTERNAL_ERROR
        elif http_exc.status_code == 500:
            code = JsonRpcErrorCode.INTERNAL_ERROR
        else:
            code = JsonRpcErrorCode.SERVER_ERROR
        
        error_response = create_jsonrpc_error_response(
            code=code
            , message=http_exc.detail
            , request_id=req.id
        )
        
        if should_use_sse(request):
            return StreamingResponse(
                sse_generator(error_response)
                , media_type="text/event-stream"
                , headers={
                    "Cache-Control": "no-cache"
                    , "Connection": "keep-alive"
                    , "X-Accel-Buffering": "no"
                }
            )
        
        return JSONResponse(status_code=200, content=error_response)
    except Exception as e:
        logger.error(f"Unexpected error in mcp_endpoint: {e}", exc_info=True)
        
        # Return JSON-RPC error response
        error_response = create_jsonrpc_error_response(
            code=JsonRpcErrorCode.INTERNAL_ERROR
            , message="Internal server error"
            , data={"error": str(e)}
            , request_id=req.id
        )
        
        if should_use_sse(request):
            return StreamingResponse(
                sse_generator(error_response)
                , media_type="text/event-stream"
                , headers={
                    "Cache-Control": "no-cache"
                    , "Connection": "keep-alive"
                    , "X-Accel-Buffering": "no"
                }
            )
        
        return JSONResponse(status_code=200, content=error_response)

@app.options("/mcp")
@app.options("/messages")
@app.options("/mcp/v1")
async def mcp_options():
    """OPTIONS handler for MCP endpoints
    
    Supports CORS preflight requests and advertises MCP server capabilities.
    Returns headers indicating supported methods, content types, and MCP protocol version.
    """
    return Response(
        status_code=200
        , headers={
            "Allow": "POST, OPTIONS"
            , "Accept": "application/json, text/event-stream"
            , "Content-Type": "application/json"
            , "X-MCP-Protocol-Version": "2025-11-25"
            , "X-MCP-Server-Name": "redox-http-proxy"
            , "X-MCP-Server-Version": "1.0.0"
            , "X-MCP-Capabilities": "tools,resources"
            , "Access-Control-Allow-Methods": "POST, OPTIONS"
            , "Access-Control-Allow-Headers": "Content-Type, Accept, Authorization"
        }
    )

# ============================================================================
# Main Entry Point
# ============================================================================
logger.info("Module initialization complete, ready to serve!")

def main() -> None:
    """Main entry point for running the application"""
    logger.info("main() function called, starting uvicorn...")
    try:
        import uvicorn
        uvicorn.run(
            app
            , host="0.0.0.0"
            , port=8000
            , log_level="info"
            , access_log=True
        )
    except Exception as e:
        logger.error(f"ERROR in main(): {e}", exc_info=True)
        raise

if __name__ == "__main__":
    logger.info("Running as __main__")
    main()

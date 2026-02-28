# redox_http_proxy.py
import asyncio
import json
import os
import signal
import subprocess
import sys
import tempfile
import traceback
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional, List

from databricks.sdk import WorkspaceClient
from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel

print(f"[redox-proxy] ALL ENV VARS: {list(os.environ.keys())}", file=sys.stderr)

def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    print(f"[redox-proxy] UNCAUGHT EXCEPTION: {exc_type.__name__}: {exc_value}", file=sys.stderr)
    print(f"[redox-proxy] Traceback:", file=sys.stderr)
    traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stderr)

sys.excepthook = handle_exception

w = WorkspaceClient()

REDOX_BINARY_PATH = None

def initialize_binary():
    global REDOX_BINARY_PATH
    try:
        VOLUME_BINARY_PATH = f"{os.environ.get('REDOX_BINARY_VOLUME')}/redox-mcp-linux-x64")
        print(f"[redox-proxy] Downloading binary from volume: {VOLUME_BINARY_PATH}", file=sys.stderr)
        temp_binary = tempfile.NamedTemporaryFile(
            mode='wb'
            , delete=False
            , suffix='-redox-mcp'
        )
        temp_binary_path = temp_binary.name
        print(f"[redox-proxy] Initiating download...", file=sys.stderr)
        response = w.files.download(VOLUME_BINARY_PATH)
        print(f"[redox-proxy] Download complete, writing to temp file...", file=sys.stderr)
        temp_binary.write(response.contents.read())
        temp_binary.close()
        print(f"[redox-proxy] Binary downloaded to: {temp_binary_path}", file=sys.stderr)
        os.chmod(temp_binary_path, 0o755)
        print(f"[redox-proxy] Set executable permissions on binary", file=sys.stderr)
        REDOX_BINARY_PATH = temp_binary_path
        print(f"[redox-proxy] Binary ready at: {REDOX_BINARY_PATH}", file=sys.stderr)
        print(f"[redox-proxy] Testing binary execution...", file=sys.stderr)
        try:
            test_result = subprocess.run(
                [REDOX_BINARY_PATH, "--version"],
                capture_output=True,
                text=True,
                timeout=5
            )
            print(f"[redox-proxy] Binary test - return code: {test_result.returncode}", file=sys.stderr)
            print(f"[redox-proxy] Binary test - stdout: {test_result.stdout}", file=sys.stderr)
            print(f"[redox-proxy] Binary test - stderr: {test_result.stderr}", file=sys.stderr)
        except Exception as test_e:
            print(f"[redox-proxy] WARNING: Binary test failed: {test_e}", file=sys.stderr)
    except Exception as e:
        print(f"[redox-proxy] ERROR initializing binary: {e}", file=sys.stderr)
        print(f"[redox-proxy] Traceback: {traceback.format_exc()}", file=sys.stderr)
        raise

def initialize_secrets():
    try:
        print(f"[redox-proxy] Validating required environment variables...", file=sys.stderr)
        
        # Define all required environment variables
        required_vars = [
            "REDOX_CLIENT_ID"
            , "REDOX_PRIVATE_KEY"
            , "REDOX_PUBLIC_KEY_ID"
            , "OAUTH_PRIVATE_KEY"
            , "OAUTH_CLIENT_ID"
            , "OAUTH_KEY_ID"
        ]
        
        # Validate each required variable
        missing_vars = []
        empty_vars = []
        
        for var_name in required_vars:
            value = os.environ.get(var_name)
            if value is None:
                missing_vars.append(var_name)
                print(f"[redox-proxy] ERROR: {var_name} is not set", file=sys.stderr)
            elif len(value) == 0:
                empty_vars.append(var_name)
                print(f"[redox-proxy] ERROR: {var_name} is empty", file=sys.stderr)
            else:
                print(f"[redox-proxy] ✓ {var_name} validated (length: {len(value)})", file=sys.stderr)
        
        # Report all validation errors at once
        if missing_vars or empty_vars:
            error_messages = []
            if missing_vars:
                error_messages.append(f"Missing environment variables: {', '.join(missing_vars)}")
            if empty_vars:
                error_messages.append(f"Empty environment variables: {', '.join(empty_vars)}")
            raise ValueError(". ".join(error_messages))
        
        # Retrieve validated values
        REDOX_CLIENT_ID = os.environ.get("REDOX_CLIENT_ID")
        REDOX_PRIVATE_KEY = os.environ.get("REDOX_PRIVATE_KEY")
        REDOX_PUBLIC_KEY_ID = os.environ.get("REDOX_PUBLIC_KEY_ID")
        OAUTH_PRIVATE_KEY = os.environ.get("OAUTH_PRIVATE_KEY")
        OAUTH_CLIENT_ID = os.environ.get("OAUTH_CLIENT_ID")
        OAUTH_KEY_ID = os.environ.get("OAUTH_KEY_ID")
        
        print(f"[redox-proxy] All required environment variables validated successfully", file=sys.stderr)
        
    except Exception as e:
        print(f"[redox-proxy] ERROR initializing secrets: {e}", file=sys.stderr)
        print(f"[redox-proxy] Traceback: {traceback.format_exc()}", file=sys.stderr)
        raise

print(f"[redox-proxy] Starting initialization...", file=sys.stderr)
try:
    initialize_secrets()
    initialize_binary()
    print(f"[redox-proxy] Initialization complete!", file=sys.stderr)
except Exception as e:
    print(f"[redox-proxy] FATAL: Initialization failed: {e}", file=sys.stderr)
    raise

class JsonRpcRequest(BaseModel):
    jsonrpc: str
    method: str
    id: Optional[int | str] = None
    params: Optional[Dict[str, Any]] = None

print(f"[redox-proxy] Defining RedoxMCPProcess class...", file=sys.stderr)

class RedoxMCPProcess:
    def __init__(self, cmd: Optional[List[str]] = None):
        print(f"[redox-proxy] Initializing RedoxMCPProcess...", file=sys.stderr)
        if cmd is None:
            cmd = [REDOX_BINARY_PATH]
        self._cmd: List[str] = [str(c) for c in cmd if c is not None]
        self._proc: Optional[subprocess.Popen] = None
        self._pending: dict[Any, asyncio.Future] = {}
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._tools_cache: Optional[List[Dict[str, Any]]] = None
        print(f"[redox-proxy] RedoxMCPProcess initialized with command: {self._cmd}", file=sys.stderr)

    async def start(self) -> None:
        if self._proc is not None:
            print(f"[redox-proxy] MCP process already running, skipping start", file=sys.stderr)
            return
        if self._loop is None:
            self._loop = asyncio.get_running_loop()
            print(f"[redox-proxy] Event loop acquired", file=sys.stderr)
        print(f"[redox-proxy] Starting MCP process with command: {self._cmd}", file=sys.stderr)
        print(f"[redox-proxy] Environment variables being passed:", file=sys.stderr)
        
        # Copy current environment
        env = os.environ.copy()
        
        # Log all OAuth-related env vars that are set
        oauth_vars = [k for k in env.keys() if 
                     'CLIENT' in k.upper() or 
                     'KEY' in k.upper() or 
                     'OAUTH' in k.upper() or 
                     'REDOX' in k.upper()]
        for var in sorted(oauth_vars):
            value = env[var]
            if 'PATH' in var:
                print(f"[redox-proxy]   {var}: {value}", file=sys.stderr)
            else:
                print(f"[redox-proxy]   {var}: {value[:20]}... (length={len(value)})", file=sys.stderr)
        
        try:
            self._proc = subprocess.Popen(
                self._cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=0,
                env=env,
            )
            print(f"[redox-proxy] MCP process started with PID: {self._proc.pid}", file=sys.stderr)
            await asyncio.sleep(0.5)
            if self._proc.poll() is not None:
                exit_code = self._proc.returncode
                stderr_output = self._proc.stderr.read().decode('utf-8') if self._proc.stderr else "No stderr"
                stdout_output = self._proc.stdout.read().decode('utf-8') if self._proc.stdout else "No stdout"
                print(f"[redox-proxy] ERROR: MCP process exited immediately with code {exit_code}", file=sys.stderr)
                print(f"[redox-proxy] STDERR: {stderr_output}", file=sys.stderr)
                print(f"[redox-proxy] STDOUT: {stdout_output}", file=sys.stderr)
                raise RuntimeError(f"MCP process failed to start (exit code {exit_code}): {stderr_output}")
        except Exception as e:
            print(f"[redox-proxy] ERROR starting subprocess: {e}", file=sys.stderr)
            print(f"[redox-proxy] Traceback: {traceback.format_exc()}", file=sys.stderr)
            raise
        if self._proc.stdin is None or self._proc.stdout is None:
            raise RuntimeError("Failed to open pipes to redox-mcp")
        print(f"[redox-proxy] MCP process running, starting read loop...", file=sys.stderr)
        self._loop.create_task(self._read_loop())
        self._loop.create_task(self._stderr_loop())

    async def _stderr_loop(self) -> None:
        if self._proc is None or self._proc.stderr is None:
            return
        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        await self._loop.connect_read_pipe(lambda: protocol, self._proc.stderr)
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                line = line.decode("utf-8").strip()
                if line:
                    print(f"[redox-mcp STDERR] {line}", file=sys.stderr)
        except Exception as e:
            print(f"[redox-proxy] Error in stderr loop: {e}", file=sys.stderr)

    def _sanitize_json_response(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        try:
            sanitized = json.loads(json.dumps(msg))
            return sanitized
        except Exception as e:
            print(f"[redox-proxy] Warning: Failed to sanitize JSON: {e}", file=sys.stderr)
            return msg

    async def _read_loop(self) -> None:
        assert self._proc is not None and self._proc.stdout is not None
        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        await self._loop.connect_read_pipe(lambda: protocol, self._proc.stdout)
        try:
            while True:
                line = await reader.readline()
                if not line:
                    print(f"[redox-proxy] Read loop: EOF reached", file=sys.stderr)
                    break
                line = line.decode("utf-8").strip()
                if not line:
                    continue
                print(f"[redox-proxy] Raw line from MCP: {repr(line[:200])}", file=sys.stderr)
                try:
                    msg = json.loads(line)
                    msg = self._sanitize_json_response(msg)
                    print(f"[redox-proxy] Parsed JSON message: {json.dumps(msg)[:200]}...", file=sys.stderr)
                except json.JSONDecodeError as je:
                    print(f"[redox-proxy] JSON decode error: {je}", file=sys.stderr)
                    print(f"[redox-proxy] Problematic line: {repr(line)}", file=sys.stderr)
                    continue
                rpc_id = msg.get("id")
                if rpc_id is not None and rpc_id in self._pending:
                    fut = self._pending.pop(rpc_id)
                    if not fut.done():
                        fut.set_result(msg)
                else:
                    print(f"[redox-proxy] Unmatched/notification message ID: {rpc_id}", file=sys.stderr)
        except Exception as e:
            print(f"[redox-proxy] Error in read loop: {e}", file=sys.stderr)
            print(f"[redox-proxy] Traceback: {traceback.format_exc()}", file=sys.stderr)
        finally:
            print(f"[redox-proxy] Read loop terminated", file=sys.stderr)
            for fut in self._pending.values():
                if not fut.done():
                    fut.set_exception(RuntimeError("redox-mcp process terminated"))
            self._pending.clear()

    async def send(self, request: Dict[str, Any]) -> Dict[str, Any]:
        assert self._proc is not None and self._proc.stdin is not None
        rpc_id = request.get("id")
        data = json.dumps(request) + "\n"
        print(f"[redox-proxy] Sending COMPLETE request (not truncated):", file=sys.stderr)
        print(json.dumps(request, indent=2), file=sys.stderr)
        print(f"[redox-proxy] Request length: {len(data)} bytes", file=sys.stderr)
        self._proc.stdin.write(data.encode("utf-8"))
        self._proc.stdin.flush()
        
        # Debug: Check if process is still alive after sending
        print(f"[redox-proxy] Process status after send: {self._proc.poll()}", file=sys.stderr)
        
        if rpc_id is None:
            return {}
        fut: asyncio.Future = self._loop.create_future()
        self._pending[rpc_id] = fut
        try:
            resp = await asyncio.wait_for(fut, timeout=120.0)
            print(f"[redox-proxy] Received response: {json.dumps(resp)[:200]}...", file=sys.stderr)
            return resp
        except asyncio.TimeoutError:
            print(f"[redox-proxy] Timeout waiting for response to request ID: {rpc_id}", file=sys.stderr)
            raise HTTPException(status_code=504, detail=f"Timeout waiting for MCP response")

    def is_alive(self) -> bool:
        """Check if the MCP process is running"""
        return self._proc is not None and self._proc.poll() is None

    async def list_tools(self) -> List[Dict[str, Any]]:
        """Query the MCP server for available tools"""
        if self._tools_cache is not None:
            return self._tools_cache
        
        response = await self.send({
            "jsonrpc": "2.0",
            "method": "tools/list",
            "id": "list_tools_req",
            "params": {}
        })
        
        if "error" in response:
            print(f"[redox-proxy] Error listing tools: {response['error']}", file=sys.stderr)
            raise HTTPException(status_code=500, detail=f"MCP error: {response['error']}")
        
        tools = response.get("result", {}).get("tools", [])
        self._tools_cache = tools
        return tools

    async def stop(self) -> None:
        if self._proc is None:
            return
        try:
            print(f"[redox-proxy] Stopping MCP process...", file=sys.stderr)
            self._proc.send_signal(signal.SIGTERM)
        except Exception as e:
            print(f"[redox-proxy] Error stopping process: {e}", file=sys.stderr)
        self._proc = None

print(f"[redox-proxy] Creating RedoxMCPProcess instance...", file=sys.stderr)
try:
    redox_proc = RedoxMCPProcess()
    print(f"[redox-proxy] RedoxMCPProcess instance created successfully", file=sys.stderr)
except Exception as e:
    print(f"[redox-proxy] ERROR creating RedoxMCPProcess: {e}", file=sys.stderr)
    print(f"[redox-proxy] Traceback: {traceback.format_exc()}", file=sys.stderr)
    raise

print(f"[redox-proxy] Defining lifespan context manager...", file=sys.stderr)

@asynccontextmanager
async def lifespan(app: FastAPI):
    print(f"[redox-proxy] FastAPI lifespan startup BEGIN", file=sys.stderr)
    sys.stderr.flush()
    try:
        print(f"[redox-proxy] About to call redox_proc.start()...", file=sys.stderr)
        sys.stderr.flush()
        await redox_proc.start()
        print(f"[redox-proxy] Redox MCP process started successfully", file=sys.stderr)
        sys.stderr.flush()
    except Exception as e:
        print(f"[redox-proxy] ERROR starting redox process: {e}", file=sys.stderr)
        print(f"[redox-proxy] Traceback: {traceback.format_exc()}", file=sys.stderr)
        sys.stderr.flush()
        raise
    print(f"[redox-proxy] Lifespan startup complete, yielding...", file=sys.stderr)
    sys.stderr.flush()
    yield
    print(f"[redox-proxy] FastAPI lifespan shutdown BEGIN", file=sys.stderr)
    sys.stderr.flush()
    await redox_proc.stop()
    print(f"[redox-proxy] FastAPI lifespan shutdown COMPLETE", file=sys.stderr)
    sys.stderr.flush()

print(f"[redox-proxy] Creating FastAPI app...", file=sys.stderr)
try:
    app = FastAPI(lifespan=lifespan)
    print(f"[redox-proxy] FastAPI app created successfully", file=sys.stderr)
except Exception as e:
    print(f"[redox-proxy] ERROR creating FastAPI app: {e}", file=sys.stderr)
    print(f"[redox-proxy] Traceback: {traceback.format_exc()}", file=sys.stderr)
    raise

@app.get("/")
async def root():
    """Root endpoint with basic info"""
    return {
        "service": "Redox MCP HTTP Proxy"
        , "status": "running"
        , "mcp_process_alive": redox_proc.is_alive()
        , "endpoints": {
            "health": "/api/v1/health"
            , "tools": "/api/v1/tools"
            , "debug_env": "/api/v1/debug/env"
            , "mcp": "/mcp"
        }
    }

@app.get("/api/v1/health")
async def health_check():
    """Health check endpoint for monitoring"""
    is_alive = redox_proc.is_alive()
    
    if not is_alive:
        return Response(
            content=json.dumps({
                "status": "unhealthy"
                , "mcp_process": "stopped"
                , "message": "MCP process is not running"
            })
            , status_code=503
            , media_type="application/json"
        )
    
    return {
        "status": "healthy"
        , "mcp_process": "running"
        , "redox_api_endpoint": "https://api.redoxengine.com/"
    }

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
        if 'PATH' in k:
            sanitized[k] = v  # Show paths in full
        else:
            sanitized[k] = f"{v[:10]}...{v[-10:]} (length={len(v)})"  # Show partial for secrets
    
    return {
        "environment_variables": sanitized
        , "count": len(oauth_vars)
        , "note": "Sensitive values are partially masked"
    }

@app.get("/api/v1/tools")
async def list_tools():
    """List all available tools from the MCP server"""
    try:
        await redox_proc.start()
        tools = await redox_proc.list_tools()
        return {
            "tools": tools
            , "count": len(tools)
            , "redox_api_connected": True
        }
    except Exception as e:
        print(f"[redox-proxy] Error listing tools: {e}", file=sys.stderr)
        print(f"[redox-proxy] Traceback: {traceback.format_exc()}", file=sys.stderr)
        raise HTTPException(
            status_code=500
            , detail=f"Failed to list tools. This may indicate authentication issues with Redox API: {str(e)}"
        )

@app.post("/mcp")
async def mcp_endpoint(req: JsonRpcRequest) -> Dict[str, Any]:
    print(f"[redox-proxy] MCP endpoint called with method: {req.method}", file=sys.stderr)
    await redox_proc.start()
    request_dict = req.model_dump(exclude_none=True)
    try:
        resp = await redox_proc.send(request_dict)
        print(f"[redox-proxy] Returning response: {json.dumps(resp)[:200]}...", file=sys.stderr)
        
        # Check for errors in the response and provide better debugging
        if "error" in resp:
            error_detail = resp["error"]
            print(f"[redox-proxy] MCP returned error: {error_detail}", file=sys.stderr)
            
            # Check if it's an authentication error
            error_message = error_detail.get("message", "")
            if "auth" in error_message.lower() or "unauthorized" in error_message.lower():
                print(f"[redox-proxy] AUTHENTICATION ERROR detected with Redox API", file=sys.stderr)
        
        return resp
    except Exception as e:
        print(f"[redox-proxy] Error in mcp_endpoint: {e}", file=sys.stderr)
        print(f"[redox-proxy] Traceback: {traceback.format_exc()}", file=sys.stderr)
        raise HTTPException(status_code=500, detail=str(e))

print(f"[redox-proxy] Module initialization complete, ready to serve!", file=sys.stderr)
sys.stderr.flush()

def main() -> None:
    print(f"[redox-proxy] main() function called, starting uvicorn...", file=sys.stderr)
    sys.stderr.flush()
    try:
        import uvicorn
        print(f"[redox-proxy] Uvicorn imported, calling run()...", file=sys.stderr)
        sys.stderr.flush()
        uvicorn.run(app, host="0.0.0.0", port=8000, log_level="debug")
    except Exception as e:
        print(f"[redox-proxy] ERROR in main(): {e}", file=sys.stderr)
        print(f"[redox-proxy] Traceback: {traceback.format_exc()}", file=sys.stderr)
        sys.stderr.flush()
        raise

if __name__ == "__main__":
    print(f"[redox-proxy] Running as __main__", file=sys.stderr)
    sys.stderr.flush()
    main()

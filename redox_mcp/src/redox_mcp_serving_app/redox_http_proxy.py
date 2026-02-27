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
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

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
        VOLUME_BINARY_PATH = os.environ.get("REDOX_BINARY_PATH", "/Volumes/mkgs/redox/bin/redox-mcp")
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
        SECRET_SCOPE_NAME = os.environ.get("SECRET_SCOPE_NAME", "redox_oauth_keys")
        if not SECRET_SCOPE_NAME:
            raise ValueError("SECRET_SCOPE_NAME environment variable is required but was not set")
        print(f"[redox-proxy] Using secret scope: {SECRET_SCOPE_NAME}", file=sys.stderr)
        print(f"[redox-proxy] Retrieving private key...", file=sys.stderr)
        PRIVATE_KEY = w.secrets.get_secret(scope=SECRET_SCOPE_NAME, key="private_key").value
        print(f"[redox-proxy] Retrieving KID...", file=sys.stderr)
        KID = w.secrets.get_secret(scope=SECRET_SCOPE_NAME, key="kid").value
        print(f"[redox-proxy] Retrieving client ID...", file=sys.stderr)
        CLIENT_ID = w.secrets.get_secret(scope=SECRET_SCOPE_NAME, key="client_id").value
        os.environ["OAUTH_CLIENT_ID"] = CLIENT_ID
        os.environ["OAUTH_KEY_ID"] = KID
        print(f"[redox-proxy] Secrets retrieved and environment variables set", file=sys.stderr)
        print(f"[redox-proxy] Writing private key to temp file...", file=sys.stderr)
        temp_key_file = tempfile.NamedTemporaryFile(
            mode='w'
            , delete=False
            , suffix='.pem'
        )
        temp_key_file.write(PRIVATE_KEY)
        temp_key_file.close()
        os.chmod(temp_key_file.name, 0o600)
        os.environ["OAUTH_KEY_PATH"] = temp_key_file.name
        print(f"[redox-proxy] Private key written to: {temp_key_file.name}", file=sys.stderr)
    except Exception as e:
        print(f"[redox-proxy] ERROR initializing secrets: {e}", file=sys.stderr)
        print(f"[redox-proxy] Traceback: {traceback.format_exc()}", file=sys.stderr)
        raise

print(f"[redox-proxy] Starting initialization...", file=sys.stderr)
try:
    initialize_binary()
    initialize_secrets()
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
        print(f"[redox-proxy] RedoxMCPProcess initialized with command: {self._cmd}", file=sys.stderr)

    async def start(self) -> None:
        if self._proc is not None:
            print(f"[redox-proxy] MCP process already running, skipping start", file=sys.stderr)
            return
        if self._loop is None:
            self._loop = asyncio.get_running_loop()
            print(f"[redox-proxy] Event loop acquired", file=sys.stderr)
        print(f"[redox-proxy] Starting MCP process with command: {self._cmd}", file=sys.stderr)
        print(f"[redox-proxy] Environment variables:", file=sys.stderr)
        print(f"[redox-proxy]   OAUTH_CLIENT_ID: {os.environ.get('OAUTH_CLIENT_ID', 'NOT SET')[:20]}...", file=sys.stderr)
        print(f"[redox-proxy]   OAUTH_KEY_ID: {os.environ.get('OAUTH_KEY_ID', 'NOT SET')[:20]}...", file=sys.stderr)
        print(f"[redox-proxy]   OAUTH_KEY_PATH: {os.environ.get('OAUTH_KEY_PATH', 'NOT SET')}", file=sys.stderr)
        try:
            self._proc = subprocess.Popen(
                self._cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=0,
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
        print(f"[redox-proxy] Sending request: {json.dumps(request)[:200]}...", file=sys.stderr)
        self._proc.stdin.write(data.encode("utf-8"))
        self._proc.stdin.flush()
        if rpc_id is None:
            return {}
        fut: asyncio.Future = self._loop.create_future()
        self._pending[rpc_id] = fut
        try:
            resp = await asyncio.wait_for(fut, timeout=30.0)
            print(f"[redox-proxy] Received response: {json.dumps(resp)[:200]}...", file=sys.stderr)
            return resp
        except asyncio.TimeoutError:
            print(f"[redox-proxy] Timeout waiting for response to request ID: {rpc_id}", file=sys.stderr)
            raise HTTPException(status_code=504, detail=f"Timeout waiting for MCP response")

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

@app.post("/mcp")
async def mcp_endpoint(req: JsonRpcRequest) -> Dict[str, Any]:
    print(f"[redox-proxy] MCP endpoint called with method: {req.method}", file=sys.stderr)
    await redox_proc.start()
    request_dict = req.model_dump(exclude_none=True)
    try:
        resp = await redox_proc.send(request_dict)
        print(f"[redox-proxy] Returning response: {json.dumps(resp)[:200]}...", file=sys.stderr)
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
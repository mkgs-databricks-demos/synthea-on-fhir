# redox_http_proxy.py
import asyncio
import json
import os
import signal
import subprocess
import sys
import tempfile
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

REDOX_BINARY_PATH = os.environ.get("REDOX_BINARY_PATH")

# Ensure binary is executable
if REDOX_BINARY_PATH and not os.access(REDOX_BINARY_PATH, os.X_OK):
    os.chmod(REDOX_BINARY_PATH, 0o755)

SECRET_SCOPE_NAME = os.environ.get("SECRET_SCOPE_NAME")
PRIVATE_KEY = dbutils.secrets.get(scope=SECRET_SCOPE_NAME, key="private_key")
KID = dbutils.secrets.get(scope=SECRET_SCOPE_NAME, key="kid")
CLIENT_ID = dbutils.secrets.get(scope=SECRET_SCOPE_NAME, key="client_id")
os.environ["OAUTH_CLIENT_ID"] = CLIENT_ID
os.environ["OAUTH_KEY_ID"] = KID

# Write private key to temporary file for MCP server
temp_key_file = tempfile.NamedTemporaryFile(
    mode='w'
    , delete=False
    , suffix='.pem'
)
temp_key_file.write(PRIVATE_KEY)
temp_key_file.close()
os.chmod(temp_key_file.name, 0o600)  # Set restrictive permissions
os.environ["OAUTH_KEY_PATH"] = temp_key_file.name



class JsonRpcRequest(BaseModel):
    jsonrpc: str
    method: str
    id: Optional[int | str] = None
    params: Optional[Dict[str, Any]] = None

class RedoxMCPProcess:
    """
    Minimal JSON-RPC 2.0 bridge to a stdio MCP server (redox-mcp).
    Assumes one JSON object per line on stdout.
    """

    def __init__(self, cmd: Optional[list[str]] = None):
        if cmd is None:
            cmd = [REDOX_BINARY_PATH]
        self._cmd = cmd
        self._proc: Optional[subprocess.Popen[bytes]] = None
        self._pending: dict[Any, asyncio.Future] = {}
        self._loop = asyncio.get_event_loop()

    async def start(self) -> None:
        if self._proc is not None:
            return

        self._proc = subprocess.Popen(
            self._cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=0,
        )

        if self._proc.stdin is None or self._proc.stdout is None:
            raise RuntimeError("Failed to open pipes to redox-mcp")

        # Start background reader
        self._loop.create_task(self._read_loop())

    async def _read_loop(self) -> None:
        assert self._proc is not None and self._proc.stdout is not None

        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        await self._loop.connect_read_pipe(lambda: protocol, self._proc.stdout)

        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                line = line.decode("utf-8").strip()
                if not line:
                    continue

                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    print(f"[redox-proxy] Non-JSON line from redox-mcp: {line}", file=sys.stderr)
                    continue

                rpc_id = msg.get("id")
                if rpc_id is not None and rpc_id in self._pending:
                    fut = self._pending.pop(rpc_id)
                    if not fut.done():
                        fut.set_result(msg)
                else:
                    # Notifications or unsolicited messages can be logged / ignored
                    print(f"[redox-proxy] Unmatched/notification message: {msg}", file=sys.stderr)

        except Exception as e:
            print(f"[redox-proxy] Error in read loop: {e}", file=sys.stderr)
        finally:
            for fut in self._pending.values():
                if not fut.done():
                    fut.set_exception(RuntimeError("redox-mcp process terminated"))
            self._pending.clear()

    async def send(self, request: Dict[str, Any]) -> Dict[str, Any]:
        assert self._proc is not None and self._proc.stdin is not None

        rpc_id = request.get("id")
        data = json.dumps(request) + "\n"
        self._proc.stdin.write(data.encode("utf-8"))
        self._proc.stdin.flush()

        # Notifications (no id) – no response expected
        if rpc_id is None:
            return {}

        fut: asyncio.Future = self._loop.create_future()
        self._pending[rpc_id] = fut
        resp = await fut
        return resp

    async def stop(self) -> None:
        if self._proc is None:
            return
        try:
            self._proc.send_signal(signal.SIGTERM)
        except Exception:
            pass
        self._proc = None

# Global instance reused across HTTP requests
redox_proc = RedoxMCPProcess()

app = FastAPI()

@app.on_event("startup")
async def startup_event() -> None:
    await redox_proc.start()

@app.on_event("shutdown")
async def shutdown_event() -> None:
    await redox_proc.stop()

@app.post("/mcp")
async def mcp_endpoint(req: JsonRpcRequest) -> Dict[str, Any]:
    """
    Transparent MCP JSON-RPC proxy:
    - Accepts JSON-RPC 2.0 request from HTTP client
    - Sends it to redox-mcp via stdio
    - Returns redox-mcp's response unchanged
    """
    await redox_proc.start()

    request_dict = req.dict(exclude_none=True)

    try:
        resp = await redox_proc.send(request_dict)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    # For notifications, resp will be {}
    return resp

def main() -> None:
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
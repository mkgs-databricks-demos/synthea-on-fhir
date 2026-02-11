from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from jwcrypto import jwk
import json
import os
from databricks.sdk import WorkspaceClient
import sys
import uvicorn

w = WorkspaceClient()

SECRET_SCOPE_NAME = os.getenv("SECRET_SCOPE_NAME", "epic_on_fhir_oauth_keys")

# Add error handling to identify the issue
try:
	print(f"Attempting to retrieve secret from scope: {SECRET_SCOPE_NAME}", file=sys.stderr)
	PUBLIC_KEY = w.dbutils.secrets.get(scope=SECRET_SCOPE_NAME, key="public_key")
	print("Secret retrieved successfully", file=sys.stderr)
except Exception as e:
	print(f"ERROR retrieving secret: {type(e).__name__}: {e}", file=sys.stderr)
	raise

KID = w.dbutils.secrets.get(scope=SECRET_SCOPE_NAME, key="kid")
ALG = os.getenv("ALGO", "RS384")

def load_jwks():
	try:
		print("Loading JWKS...", file=sys.stderr)
		key = jwk.JWK.from_pem(PUBLIC_KEY.encode('utf-8'))
		key.use = "sig"
		key.alg = ALG
		key.kid = KID
		print("JWKS loaded successfully", file=sys.stderr)
		return {"keys": [json.loads(key.export_public())]}
	except Exception as e:
		print(f"ERROR loading JWKS: {type(e).__name__}: {e}", file=sys.stderr)
		raise

JWKS = load_jwks()

# Create FastAPI app
app = FastAPI(title="Epic JWKS Endpoint")

app.add_middleware(
    CORSMiddleware
    , allow_origins=["*"]
    , allow_credentials=False
    , allow_methods=["GET"]
    , allow_headers=["*"]
)

@app.get("/")
async def root():
	"""Root endpoint showing app info"""
	return {
		"message": "Epic JWKS Endpoint"
		, "description": "This app serves JWKS for Epic backend services."
		, "jwks_endpoint": "/.well-known/jwks.json"
		, "jwks_preview": JWKS
	}

@app.get("/.well-known/jwks.json")
async def jwks_endpoint():
	"""JWKS endpoint for Epic to consume"""
	return JSONResponse(content=JWKS)

if __name__ == "__main__":
	uvicorn.run(app, host="0.0.0.0", port=8000)

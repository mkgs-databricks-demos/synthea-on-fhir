from shiny import App, ui, render, Inputs, Outputs, Session
from shiny import reactive
from jwcrypto import jwk
import json
import os
from databricks.sdk import WorkspaceClient
import sys

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

KID = os.getenv("EPIC_KID", "fy27fde")
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

# Minimal UI just to show the JWKS; Epic will call the JSON endpoint
app_ui = ui.page_fluid(
	ui.h2("Epic JWKS endpoint"),
	ui.p("This app serves JWKS for Epic backend services."),
	ui.output_text_verbatim("jwks_preview")
)

def server(input: Inputs, output: Outputs, session: Session):

	@output
	@render.text
	def jwks_preview():
		return json.dumps(JWKS, indent=2)

	# # Expose a raw JWKS endpoint for Epic (Starlette route)
	# @session.app.router.get("/.well-known/jwks.json")
	# async def jwks_route(request):
	# 	from starlette.responses import JSONResponse
	# 	return JSONResponse(JWKS)

# app = App(app_ui, server)

# # Register route after app creation
# @app.app.router.get("/.well-known/jwks.json")
# async def jwks_route(request):
#     from starlette.responses import JSONResponse
#     return JSONResponse(JWKS)

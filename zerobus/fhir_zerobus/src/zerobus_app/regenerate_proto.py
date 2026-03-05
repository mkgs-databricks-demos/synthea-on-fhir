import os
os.environ["PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION"] = "cpp"

import subprocess
import sys

result = subprocess.run([
    sys.executable, '-m', 'grpc_tools.protoc',
    '--python_out=.',
    '--proto_path=.',
    'fhir_bundle.proto'
], capture_output=True, text=True)

print(f"Return code: {result.returncode}")
print(f"Stdout: {result.stdout}")
print(f"Stderr: {result.stderr}")

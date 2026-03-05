with open('fhir_bundle_pb2.py', 'r') as f:
    content = f.read()

# Replace version string in comment
content = content.replace('# Protobuf Python Version: 6.31.1', '# Protobuf Python Version: 5.29.6')

# Replace version numbers in ValidateProtobufRuntimeVersion call
content = content.replace(
    '''_runtime_version.ValidateProtobufRuntimeVersion(
    _runtime_version.Domain.PUBLIC,
    6,
    31,
    1,''',
    '''_runtime_version.ValidateProtobufRuntimeVersion(
    _runtime_version.Domain.PUBLIC,
    5,
    29,
    6,'''
)

with open('fhir_bundle_pb2.py', 'w') as f:
    f.write(content)

print("✓ Patched version to 5.29.6")

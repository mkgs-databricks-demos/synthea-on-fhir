## Session: 2026-04-11 09:00 UTC

### Serving Endpoint Config Update Limitation

**Problem**: User asked whether the bundle can update AI Gateway and telemetry on existing endpoints after first deployment.

**Finding**: The Serving Endpoint REST API splits configuration updates across multiple paths:

| API | What it updates |
| --- | --- |
| `POST /serving-endpoints` | Creates with full config (served_entities, traffic_config, telemetry_config, ai_gateway, tags) |
| `PUT /serving-endpoints/{name}/config` | Only served_entities and traffic_config |
| `PUT /serving-endpoints/{name}/ai-gateway` | AI Gateway settings only |
| `PATCH /serving-endpoints/{name}` | Tags only |

**Impact**: DAB bundle's serving resource sets all config on first deploy. On subsequent deploys it uses the config update API, which only handles served_entities and traffic_config — AI Gateway and telemetry require separate API calls.

**Solution**: Created `update-serving-endpoint-config.ipynb` notebook (12 cells) as a job task to apply full endpoint configuration via SDK calls:

| Cell | Purpose |
| --- | --- |
| 1-3 | Setup (parameters, SDK init) |
| 4 | Endpoint existence guard |
| 5-6 | AI Gateway create/update |
| 7-8 | Telemetry config |
| 9-10 | Tags |
| 11 | Rate limits |
| 12 | Verification |

### Files Modified (Summary)

| File | Action |
| --- | --- |
| `src/update-serving-endpoint-config.ipynb` | Created: 12-cell notebook for full endpoint config via SDK |
| `resources/epic_on_fhir_model_registration.job.yml` | Added `update_endpoint_config` task |

# Trimble Geospatial API

## Overview
The API is a thin, read-only serving layer over pre-aggregated Delta tables in
Databricks. It does not ingest raw files or run long jobs. It validates request
parameters, queries curated tables, and returns compact JSON payloads.

Base path: `/api/v1`

## Authentication
All endpoints require an API key in the `x-api-key` header.

## Swagger / OpenAPI
- Local: `http://localhost:<port>/swagger`
- Deployed: `https://<app>.azurewebsites.net/swagger`
- JSON: `https://<app>.azurewebsites.net/swagger/v1/swagger.json`

## Endpoints (public)
- Runs
  - `GET /api/v1/sites/{siteId}/runs/latest`
  - `GET /api/v1/sites/{siteId}/runs/{runId}`
- Tiles
  - `GET /api/v1/sites/{siteId}/tiles`
- Water Bodies
  - `GET /api/v1/sites/{siteId}/water-bodies`
  - `GET /api/v1/sites/{siteId}/water-bodies/{waterBodyId}`
- Building Candidates
  - `GET /api/v1/sites/{siteId}/building-candidates`
  - `GET /api/v1/sites/{siteId}/tiles/{tileId}/building-candidates/{candidateId}`

## Error format
All error responses share this schema:
```json
{
  "errorCode": "string",
  "message": "string",
  "correlationId": "string"
}
```

## Data pipeline dependency (summary)
The API serves outputs produced by the Databricks pipelines in
`databricks/pipelines`:
- `main.demo.tile_stats_v2`
- `main.demo.features_water_bodies_v2`
- `main.demo.features_building_candidates_v2`
- `main.demo.pipeline_runs`

For the full workflow description and job DAG, see the repo root `README.md`.

## Deployment

### Env settings (App Service -> Application settings)
- `DATABRICKS_HOST`: https://adb-7405613410614509.9.azuredatabricks.net
- `DATABRICKS_HTTP_PATH`: /sql/1.0/warehouses/42237f5a0be62e4e
- `DATABRICKS_AAD_SCOPE`: optional override (recommended default)
  - `2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default`
- `INTERNAL_API_KEY`: secret used for `/internal/*`
- `PUBLIC_API_KEY`: optional (defaults to internal key if unset)
- `AAD_TENANT_ID`: optional tenant override for AAD token issuance
- `MANAGED_IDENTITY_CLIENT_ID`: required for user-assigned managed identity

### Local run (Windows / PowerShell)
```powershell
az login
$env:INTERNAL_API_KEY = "<local-secret>"
dotnet run --project api/Trimble.Geospatial.Api.csproj
```

### Azure App Service (Linux)
1) Enable System-assigned Managed Identity.
2) Set application settings (above).
3) Deploy via zip deploy or GitHub Actions.

## Troubleshooting
- AADSTS500011 (invalid_resource):
  - Prefer `DATABRICKS_AAD_SCOPE = 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default`.
- 403 Forbidden from Databricks SQL:
  - Register the service principal by **Application ID** (appid), not Object ID.
  - Grant "Databricks SQL access" entitlement.
  - Grant "Can Use" on the target SQL warehouse.

## Deployment helpers

### GitHub Actions
1) Add secret `AZURE_WEBAPP_PUBLISH_PROFILE` with the Web App publish profile.
2) Use `.github/workflows/trimble-geospatial-api.yml`.

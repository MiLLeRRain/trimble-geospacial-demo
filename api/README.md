# API Boundary

Purpose
- Thin serving layer over aggregated Delta tables
- No direct access to raw point clouds

Responsibilities
- Validate query params (siteId, bbox, time)
- Query pre-aggregated tables
- Return small JSON payloads

Non-Goals
- Uploading raw files
- Long-running batch jobs

Deployment Guide

Env Settings (App Service Configuration -> Application settings)
- DATABRICKS_HOST: https://adb-7405613410614509.9.azuredatabricks.net
- DATABRICKS_HTTP_PATH: /sql/1.0/warehouses/42237f5a0be62e4e
- INTERNAL_API_KEY: set a strong secret used for /internal/* endpoints
- AAD_TENANT_ID: optional, only needed if you must pin token issuance to a tenant

Local Run (Windows / PowerShell)
1) az login
2) set environment variables and run:
   - $env:INTERNAL_API_KEY = "<local-secret>"
   - dotnet run

Azure App Service (Linux)
1) Ensure System-assigned Managed Identity is enabled.
2) Set Application settings (above).
3) Deploy using zip deploy or GitHub Actions.

Zip Deploy (simplest)
1) dotnet publish -c Release -o publish
2) Compress-Archive -Path publish\* -DestinationPath publish.zip -Force
3) az webapp deploy --resource-group trimble-geospatial-demo-rg --name trimble-geospatial-api --src-path publish.zip --type zip

GitHub Actions
1) Add secret AZURE_WEBAPP_PUBLISH_PROFILE with the Web App publish profile contents.
2) Use .github/workflows/trimble-geospatial-api.yml

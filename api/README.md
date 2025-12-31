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
- DATABRICKS_AAD_SCOPE: optional override (recommended default)
   - 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default
- INTERNAL_API_KEY: set a strong secret used for /internal/* endpoints
- AAD_TENANT_ID: optional, pins token issuance to a specific tenant (useful for AADSTS500011 / wrong-tenant issues)
- MANAGED_IDENTITY_CLIENT_ID: optional, required if using a user-assigned managed identity

Local Run (Windows / PowerShell)
1) az login
2) set environment variables and run:
   - $env:INTERNAL_API_KEY = "<local-secret>"
   - dotnet run

Azure App Service (Linux)
1) Ensure System-assigned Managed Identity is enabled.
2) Set Application settings (above).
3) Deploy using zip deploy or GitHub Actions.

Troubleshooting
- If you see AADSTS500011 (invalid_resource) for Databricks:
   - Verify the App Service is authenticating against the correct Entra tenant (set AAD_TENANT_ID).
   - Prefer DATABRICKS_AAD_SCOPE = 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default.

Zip Deploy (simplest)
1) dotnet publish -c Release -o publish
2) Compress-Archive -Path publish\* -DestinationPath publish.zip -Force
3) az webapp deploy --resource-group trimble-geospatial-demo-rg --name trimble-geospatial-api --src-path publish.zip --type zip

GitHub Actions
1) Add secret AZURE_WEBAPP_PUBLISH_PROFILE with the Web App publish profile contents.
2) Use .github/workflows/trimble-geospatial-api.yml

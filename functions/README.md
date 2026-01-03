# Functions (Orchestrator)

This folder hosts the orchestration Azure Functions.

## Step 2: Job DB connectivity smoke test

### Required App Settings (Function App)
- `JobDb__Server` (example): `trimble-geospatial-demo-sql.database.windows.net`
- `JobDb__Database` (example): `trimble-geospatial-demo-sql`

The code uses SQL client auth:
- `Authentication=Active Directory Default`

In Azure, this should use the Function App Managed Identity.

### HTTP Trigger (manual)
Function name: `jobdb-update`
- Route: `POST /api/jobdb/update?jobId=<jobId>&status=FUNCTION_TESTED`
- If `jobId` is omitted, it updates the latest job by `CreatedAtUtc`.

How to trigger after deployment:
1) Azure Portal -> Function App -> Functions -> `jobdb-update` -> **Get Function Url**.
2) Call it using the provided URL (it includes `?code=...`).

### Timer Trigger (liveness)
Function name: `jobdb-update-timer`
- Schedule: every 5 minutes
- Updates latest job status to `FUNCTION_TIMER_TICK`.

Check liveness after deployment:
- Azure Portal -> Function App -> **Log stream**
- Or Application Insights -> Logs


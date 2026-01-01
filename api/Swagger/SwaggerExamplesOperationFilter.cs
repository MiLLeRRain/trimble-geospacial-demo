using Microsoft.OpenApi.Any;
using Microsoft.OpenApi.Models;
using Swashbuckle.AspNetCore.SwaggerGen;

namespace Trimble.Geospatial.Api.Swagger;

public sealed class SwaggerExamplesOperationFilter : IOperationFilter
{
    public void Apply(OpenApiOperation operation, OperationFilterContext context)
    {
        var path = context.ApiDescription.RelativePath ?? string.Empty;

        switch (path)
        {
            case "api/v1/sites/{siteId}/runs/latest":
                ApplyExample(operation, StatusCodes.Status200OK, BuildLatestRunExample());
                ApplyExample(operation, StatusCodes.Status404NotFound, BuildRunNotFoundExample());
                break;
            case "api/v1/sites/{siteId}/runs/{runId}":
                ApplyExample(operation, StatusCodes.Status200OK, BuildRunByIdExample());
                ApplyExample(operation, StatusCodes.Status404NotFound, BuildRunNotFoundExample());
                break;
            case "api/v1/sites/{siteId}/tiles":
                ApplyExample(operation, StatusCodes.Status200OK, BuildTileListExample());
                ApplyExample(operation, StatusCodes.Status400BadRequest, BuildTileListBadRequestExample());
                break;
            case "api/v1/sites/{siteId}/water-bodies":
                ApplyExample(operation, StatusCodes.Status200OK, BuildWaterBodyListExample());
                ApplyExample(operation, StatusCodes.Status400BadRequest, BuildWaterBodyListBadRequestExample());
                break;
            case "api/v1/sites/{siteId}/water-bodies/{waterBodyId}":
                ApplyExample(operation, StatusCodes.Status200OK, BuildWaterBodyExample());
                ApplyExample(operation, StatusCodes.Status404NotFound, BuildWaterBodyNotFoundExample());
                break;
            case "api/v1/sites/{siteId}/building-candidates":
                ApplyExample(operation, StatusCodes.Status200OK, BuildBuildingCandidateListExample());
                ApplyExample(operation, StatusCodes.Status400BadRequest, BuildBuildingCandidateListBadRequestExample());
                break;
            case "api/v1/sites/{siteId}/tiles/{tileId}/building-candidates/{candidateId}":
                ApplyExample(operation, StatusCodes.Status200OK, BuildBuildingCandidateExample());
                ApplyExample(operation, StatusCodes.Status404NotFound, BuildBuildingCandidateNotFoundExample());
                break;
        }
    }

    private static void ApplyExample(OpenApiOperation operation, int statusCode, IOpenApiAny example)
    {
        var statusKey = statusCode.ToString();
        if (!operation.Responses.TryGetValue(statusKey, out var response))
        {
            return;
        }

        if (!response.Content.TryGetValue("application/json", out var content))
        {
            return;
        }

        content.Example = example;
    }

    private static IOpenApiAny BuildLatestRunExample()
    {
        return new OpenApiObject
        {
            ["siteId"] = new OpenApiString("site-123"),
            ["runId"] = new OpenApiString("run-20240115-001"),
            ["status"] = new OpenApiString("Completed"),
            ["startedAt"] = new OpenApiString("2024-01-15T09:10:00Z"),
            ["finishedAt"] = new OpenApiString("2024-01-15T09:18:45Z"),
            ["errorMessage"] = new OpenApiNull(),
            ["producedSnapshotAt"] = new OpenApiString("2024-01-15T09:20:00Z")
        };
    }

    private static IOpenApiAny BuildRunByIdExample()
    {
        return new OpenApiObject
        {
            ["siteId"] = new OpenApiString("site-123"),
            ["runId"] = new OpenApiString("run-20240114-007"),
            ["status"] = new OpenApiString("Failed"),
            ["startedAt"] = new OpenApiString("2024-01-14T20:00:00Z"),
            ["finishedAt"] = new OpenApiString("2024-01-14T20:07:30Z"),
            ["errorMessage"] = new OpenApiString("Point cloud ingestion failed."),
            ["producedSnapshotAt"] = new OpenApiNull()
        };
    }

    private static IOpenApiAny BuildRunNotFoundExample()
    {
        return new OpenApiObject
        {
            ["errorCode"] = new OpenApiString("RunNotFound"),
            ["message"] = new OpenApiString("No run 'run-unknown' found for siteId 'site-123'."),
            ["correlationId"] = new OpenApiString("2f6a6f7b5c1e45e68f52e2f5b9c3a4aa")
        };
    }

    private static IOpenApiAny BuildTileListExample()
    {
        return new OpenApiArray
        {
            new OpenApiObject
            {
                ["siteId"] = new OpenApiString("site-123"),
                ["tileId"] = new OpenApiString("tile-001"),
                ["pointCount"] = new OpenApiLong(312455),
                ["z_p50"] = new OpenApiDouble(12.4),
                ["z_p95"] = new OpenApiDouble(28.7),
                ["z_p99"] = new OpenApiDouble(31.2),
                ["waterPointRatio"] = new OpenApiDouble(0.08),
                ["isMostlyWater"] = new OpenApiBoolean(false),
                ["computedAt"] = new OpenApiString("2024-01-15T10:00:00Z")
            }
        };
    }

    private static IOpenApiAny BuildTileListBadRequestExample()
    {
        return new OpenApiObject
        {
            ["errorCode"] = new OpenApiString("InvalidOrderBy"),
            ["message"] = new OpenApiString("orderBy must be one of: tileId, pointCountDesc, waterRatioDesc, heightP99Desc, reliefDesc, computedAtDesc."),
            ["correlationId"] = new OpenApiString("b4c1f2d7a3a64b4c9f3a7dfd8f7a2c10")
        };
    }

    private static IOpenApiAny BuildWaterBodyListExample()
    {
        return new OpenApiArray
        {
            new OpenApiObject
            {
                ["waterBodyId"] = new OpenApiLong(42),
                ["areaM2"] = new OpenApiDouble(1250.5),
                ["cellCount"] = new OpenApiLong(8450),
                ["meanZ"] = new OpenApiDouble(10.1),
                ["minZ"] = new OpenApiDouble(9.8),
                ["maxZ"] = new OpenApiDouble(10.6),
                ["bboxMinCellX"] = new OpenApiInteger(120),
                ["bboxMinCellY"] = new OpenApiInteger(220),
                ["bboxMaxCellX"] = new OpenApiInteger(180),
                ["bboxMaxCellY"] = new OpenApiInteger(260),
                ["computedAt"] = new OpenApiString("2024-01-15T10:05:00Z")
            }
        };
    }

    private static IOpenApiAny BuildWaterBodyListBadRequestExample()
    {
        return new OpenApiObject
        {
            ["errorCode"] = new OpenApiString("InvalidMinAreaM2"),
            ["message"] = new OpenApiString("minAreaM2 must be 0 or greater."),
            ["correlationId"] = new OpenApiString("1c42d9e3f46f4e6fb130b8e3f7a1e2d4")
        };
    }

    private static IOpenApiAny BuildWaterBodyExample()
    {
        return new OpenApiObject
        {
            ["waterBodyId"] = new OpenApiLong(42),
            ["areaM2"] = new OpenApiDouble(1250.5),
            ["cellCount"] = new OpenApiLong(8450),
            ["meanZ"] = new OpenApiDouble(10.1),
            ["minZ"] = new OpenApiDouble(9.8),
            ["maxZ"] = new OpenApiDouble(10.6),
            ["bboxMinCellX"] = new OpenApiInteger(120),
            ["bboxMinCellY"] = new OpenApiInteger(220),
            ["bboxMaxCellX"] = new OpenApiInteger(180),
            ["bboxMaxCellY"] = new OpenApiInteger(260),
            ["computedAt"] = new OpenApiString("2024-01-15T10:05:00Z")
        };
    }

    private static IOpenApiAny BuildWaterBodyNotFoundExample()
    {
        return new OpenApiObject
        {
            ["errorCode"] = new OpenApiString("WaterBodyNotFound"),
            ["message"] = new OpenApiString("No waterBodyId '99' found for siteId 'site-123'."),
            ["correlationId"] = new OpenApiString("7f2db99a1a0849a78a8b3b6b0f6a5c21")
        };
    }

    private static IOpenApiAny BuildBuildingCandidateListExample()
    {
        return new OpenApiArray
        {
            new OpenApiObject
            {
                ["buildingCandidateId"] = new OpenApiString("cand-001"),
                ["tileId"] = new OpenApiString("tile-001"),
                ["heightAboveGround"] = new OpenApiDouble(12.6),
                ["heightRange"] = new OpenApiDouble(4.1),
                ["pointsUsed"] = new OpenApiLong(12500),
                ["cellCount"] = new OpenApiLong(240),
                ["minZ"] = new OpenApiDouble(9.9),
                ["meanZ"] = new OpenApiDouble(14.0),
                ["maxZ"] = new OpenApiDouble(17.2),
                ["computedAt"] = new OpenApiString("2024-01-15T10:10:00Z")
            }
        };
    }

    private static IOpenApiAny BuildBuildingCandidateListBadRequestExample()
    {
        return new OpenApiObject
        {
            ["errorCode"] = new OpenApiString("InvalidOrderBy"),
            ["message"] = new OpenApiString("orderBy must be one of: heightDesc, heightRangeDesc."),
            ["correlationId"] = new OpenApiString("b2c7f6d2a1c84f9199d2a64f6f4a1d2b")
        };
    }

    private static IOpenApiAny BuildBuildingCandidateExample()
    {
        return new OpenApiObject
        {
            ["buildingCandidateId"] = new OpenApiString("cand-001"),
            ["tileId"] = new OpenApiString("tile-001"),
            ["heightAboveGround"] = new OpenApiDouble(12.6),
            ["heightRange"] = new OpenApiDouble(4.1),
            ["pointsUsed"] = new OpenApiLong(12500),
            ["cellCount"] = new OpenApiLong(240),
            ["minZ"] = new OpenApiDouble(9.9),
            ["meanZ"] = new OpenApiDouble(14.0),
            ["maxZ"] = new OpenApiDouble(17.2),
            ["computedAt"] = new OpenApiString("2024-01-15T10:10:00Z")
        };
    }

    private static IOpenApiAny BuildBuildingCandidateNotFoundExample()
    {
        return new OpenApiObject
        {
            ["errorCode"] = new OpenApiString("BuildingCandidateNotFound"),
            ["message"] = new OpenApiString("No candidate 'cand-404' found for siteId 'site-123' and tileId 'tile-001'."),
            ["correlationId"] = new OpenApiString("a1b2c3d4e5f6478aa9b8c7d6e5f4a3b2")
        };
    }
}

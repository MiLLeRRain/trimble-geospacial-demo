using Microsoft.OpenApi.Models;
using Swashbuckle.AspNetCore.SwaggerGen;
using Trimble.Geospatial.Api.Models;

namespace Trimble.Geospatial.Api.Swagger;

public sealed class SwaggerResponsesOperationFilter : IOperationFilter
{
    public void Apply(OpenApiOperation operation, OperationFilterContext context)
    {
        var path = context.ApiDescription.RelativePath ?? string.Empty;

        switch (path)
        {
            case "api/v1/sites/{siteId}/runs/latest":
                AddResponse(operation, context, StatusCodes.Status200OK, "Latest pipeline run status.", typeof(PipelineRunStatusResponse));
                AddCommonErrors(operation, context, includeBadRequest: false, includeNotFound: true);
                break;
            case "api/v1/sites/{siteId}/runs/{runId}":
                AddResponse(operation, context, StatusCodes.Status200OK, "Pipeline run status.", typeof(PipelineRunStatusResponse));
                AddCommonErrors(operation, context, includeBadRequest: false, includeNotFound: true);
                break;
            case "api/v1/sites/{siteId}/tiles":
                AddResponse(operation, context, StatusCodes.Status200OK, "Tile statistics for the requested site.", typeof(IReadOnlyList<TileStatsResponse>));
                AddCommonErrors(operation, context, includeBadRequest: true, includeNotFound: false);
                break;
            case "api/v1/sites/{siteId}/water-bodies":
                AddResponse(operation, context, StatusCodes.Status200OK, "Water bodies for the requested site.", typeof(IReadOnlyList<WaterBodyResponse>));
                AddCommonErrors(operation, context, includeBadRequest: true, includeNotFound: false);
                break;
            case "api/v1/sites/{siteId}/water-bodies/{waterBodyId}":
                AddResponse(operation, context, StatusCodes.Status200OK, "Water body details.", typeof(WaterBodyResponse));
                AddCommonErrors(operation, context, includeBadRequest: true, includeNotFound: true);
                break;
            case "api/v1/sites/{siteId}/building-candidates":
                AddResponse(operation, context, StatusCodes.Status200OK, "Building candidates for the requested site.", typeof(IReadOnlyList<BuildingCandidateResponse>));
                AddCommonErrors(operation, context, includeBadRequest: true, includeNotFound: false);
                break;
            case "api/v1/sites/{siteId}/tiles/{tileId}/building-candidates/{candidateId}":
                AddResponse(operation, context, StatusCodes.Status200OK, "Building candidate details.", typeof(BuildingCandidateResponse));
                AddCommonErrors(operation, context, includeBadRequest: false, includeNotFound: true);
                break;
        }
    }

    private static void AddCommonErrors(OpenApiOperation operation, OperationFilterContext context, bool includeBadRequest, bool includeNotFound)
    {
        AddResponse(operation, context, StatusCodes.Status401Unauthorized, "Missing or invalid API key.", typeof(ApiError));

        if (includeBadRequest)
        {
            AddResponse(operation, context, StatusCodes.Status400BadRequest, "Invalid request parameters.", typeof(ApiError));
        }

        if (includeNotFound)
        {
            AddResponse(operation, context, StatusCodes.Status404NotFound, "The requested resource was not found.", typeof(ApiError));
        }

        AddResponse(operation, context, StatusCodes.Status503ServiceUnavailable, "Databricks SQL is temporarily unavailable.", typeof(ApiError));
        AddResponse(operation, context, StatusCodes.Status502BadGateway, "Databricks SQL query failed.", typeof(ApiError));
    }

    private static void AddResponse(OpenApiOperation operation, OperationFilterContext context, int statusCode, string description, Type responseType)
    {
        var statusKey = statusCode.ToString();
        var schema = context.SchemaGenerator.GenerateSchema(responseType, context.SchemaRepository);

        operation.Responses[statusKey] = new OpenApiResponse
        {
            Description = description,
            Content = new Dictionary<string, OpenApiMediaType>
            {
                ["application/json"] = new()
                {
                    Schema = schema
                }
            }
        };
    }
}

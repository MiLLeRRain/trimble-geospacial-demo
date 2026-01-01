using Microsoft.OpenApi.Any;
using Microsoft.OpenApi.Models;
using Swashbuckle.AspNetCore.SwaggerGen;

namespace Trimble.Geospatial.Api.Swagger;

public sealed class SwaggerParameterFilter : IParameterFilter
{
    public void Apply(OpenApiParameter parameter, ParameterFilterContext context)
    {
        var path = context.ApiDescription.RelativePath ?? string.Empty;
        var name = parameter.Name;

        switch (name)
        {
            case "siteId":
                parameter.Description ??= "Site identifier for the dataset.";
                parameter.Example = new OpenApiString("site-123");
                break;
            case "runId":
                parameter.Description ??= "Pipeline run identifier.";
                parameter.Example = new OpenApiString("run-20240115-001");
                break;
            case "tileId":
                parameter.Description ??= "Tile identifier within the site.";
                parameter.Example = new OpenApiString("tile-001");
                break;
            case "waterBodyId":
                parameter.Description ??= "Water body identifier.";
                parameter.Example = new OpenApiInteger(42);
                break;
            case "candidateId":
                parameter.Description ??= "Building candidate identifier.";
                parameter.Example = new OpenApiString("cand-001");
                break;
            case "skipMostlyWater":
                parameter.Description ??= "Exclude tiles that are mostly water.";
                parameter.Example = new OpenApiBoolean(true);
                parameter.Schema.Default = new OpenApiBoolean(false);
                break;
            case "minPointCount":
                parameter.Description ??= "Minimum point count to include.";
                parameter.Example = new OpenApiLong(5000);
                parameter.Schema.Minimum = 0;
                break;
            case "limit":
                ApplyLimit(parameter, path);
                break;
            case "offset":
                parameter.Description ??= "Zero-based offset into the result set.";
                parameter.Example = new OpenApiInteger(0);
                parameter.Schema.Minimum = 0;
                parameter.Schema.Default = new OpenApiInteger(0);
                break;
            case "orderBy":
                ApplyOrderBy(parameter, path);
                break;
            case "minAreaM2":
                parameter.Description ??= "Minimum water body area in square meters.";
                parameter.Example = new OpenApiDouble(250.5);
                parameter.Schema.Minimum = 0;
                break;
            case "minHeight":
                parameter.Description ??= "Minimum height above ground in meters.";
                parameter.Example = new OpenApiDouble(2.5);
                parameter.Schema.Minimum = 0;
                break;
        }
    }

    private static void ApplyLimit(OpenApiParameter parameter, string path)
    {
        parameter.Description ??= "Maximum number of items to return.";
        parameter.Example = new OpenApiInteger(100);
        parameter.Schema.Minimum = 1;

        if (path.Contains("/tiles", StringComparison.OrdinalIgnoreCase))
        {
            parameter.Schema.Maximum = 2000;
            parameter.Schema.Default = new OpenApiInteger(100);
            return;
        }

        parameter.Schema.Maximum = 1000;
        parameter.Schema.Default = new OpenApiInteger(100);
    }

    private static void ApplyOrderBy(OpenApiParameter parameter, string path)
    {
        parameter.Description ??= "Sort order for the results.";

        if (path.Contains("/tiles", StringComparison.OrdinalIgnoreCase))
        {
            parameter.Schema.Enum = new List<IOpenApiAny>
            {
                new OpenApiString("tileId"),
                new OpenApiString("pointCountDesc"),
                new OpenApiString("waterRatioDesc"),
                new OpenApiString("heightP99Desc"),
                new OpenApiString("reliefDesc"),
                new OpenApiString("computedAtDesc")
            };
            parameter.Example = new OpenApiString("pointCountDesc");
            return;
        }

        if (path.Contains("/water-bodies", StringComparison.OrdinalIgnoreCase))
        {
            parameter.Schema.Enum = new List<IOpenApiAny>
            {
                new OpenApiString("areaDesc")
            };
            parameter.Example = new OpenApiString("areaDesc");
            return;
        }

        if (path.Contains("/building-candidates", StringComparison.OrdinalIgnoreCase))
        {
            parameter.Schema.Enum = new List<IOpenApiAny>
            {
                new OpenApiString("heightDesc"),
                new OpenApiString("heightRangeDesc")
            };
            parameter.Example = new OpenApiString("heightDesc");
        }
    }
}

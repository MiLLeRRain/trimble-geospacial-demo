using Microsoft.OpenApi.Models;
using Swashbuckle.AspNetCore.SwaggerGen;

namespace Trimble.Geospatial.Api.Swagger;

public sealed class SwaggerTagsDocumentFilter : IDocumentFilter
{
    public void Apply(OpenApiDocument swaggerDoc, DocumentFilterContext context)
    {
        swaggerDoc.Tags = new List<OpenApiTag>
        {
            new()
            {
                Name = "Runs",
                Description = "Pipeline run status and metadata."
            },
            new()
            {
                Name = "Tiles",
                Description = "Tile-level LiDAR statistics."
            },
            new()
            {
                Name = "Water Bodies",
                Description = "Detected water body features."
            },
            new()
            {
                Name = "Building Candidates",
                Description = "Detected building candidate features."
            }
        };
    }
}

using Microsoft.OpenApi.Models;
using Swashbuckle.AspNetCore.SwaggerGen;

namespace Trimble.Geospatial.Api.Swagger;

public sealed class SwaggerTagsDocumentFilter : IDocumentFilter
{
    public void Apply(OpenApiDocument swaggerDoc, DocumentFilterContext context)
    {
        var tagDescriptions = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["Runs"] = "Pipeline run status and metadata.",
            ["Tiles"] = "Tile-level LiDAR statistics.",
            ["Water Bodies"] = "Detected water body features.",
            ["Building Candidates"] = "Detected building candidate features."
        };

        var tagMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["Runs"] = "Runs",
            ["Tiles"] = "Tiles",
            ["Water Bodies"] = "Water Bodies",
            ["Building Candidates"] = "Building Candidates"
        };

        foreach (var path in swaggerDoc.Paths.Values)
        {
            foreach (var operation in path.Operations.Values)
            {
                if (operation.Tags.Count == 0)
                {
                    continue;
                }

                var remapped = operation.Tags
                    .Select(tag => tagMap.TryGetValue(tag.Name, out var mapped) ? mapped : tag.Name)
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .ToList();

                operation.Tags = remapped.Select(name => new OpenApiTag { Name = name }).ToList();
            }
        }

        var distinctTags = swaggerDoc.Paths.Values
            .SelectMany(path => path.Operations.Values)
            .SelectMany(operation => operation.Tags)
            .Select(tag => tag.Name)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        swaggerDoc.Tags = distinctTags
            .Select(name => new OpenApiTag
            {
                Name = name,
                Description = tagDescriptions.TryGetValue(name, out var description) ? description : null
            })
            .ToList();
    }
}

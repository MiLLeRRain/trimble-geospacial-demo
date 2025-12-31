namespace Trimble.Geospatial.Api.Options;

public sealed class DatabricksOptions
{
    public string Host { get; init; } = "https://adb-7405613410614509.9.azuredatabricks.net";
    public string HttpPath { get; init; } = "/sql/1.0/warehouses/42237f5a0be62e4e";
    // More reliable than https://databricks.azure.net/.default across tenants.
    // Azure Databricks first-party app id: 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d
    public string AadScope { get; init; } = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default";
    public string? WarehouseId { get; init; }

    public Uri GetHostUri()
    {
        if (!Uri.TryCreate(Host, UriKind.Absolute, out var uri))
        {
            throw new InvalidOperationException("Databricks Host is not a valid absolute URI.");
        }

        return new Uri(uri.GetLeftPart(UriPartial.Authority));
    }

    public string GetWarehouseId()
    {
        if (!string.IsNullOrWhiteSpace(WarehouseId))
        {
            return WarehouseId;
        }

        if (string.IsNullOrWhiteSpace(HttpPath))
        {
            throw new InvalidOperationException("Databricks HttpPath is required to determine warehouse id.");
        }

        var segments = HttpPath.Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (segments.Length == 0)
        {
            throw new InvalidOperationException("Databricks HttpPath is invalid.");
        }

        return segments[^1];
    }
}

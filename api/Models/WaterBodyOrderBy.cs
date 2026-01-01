namespace Trimble.Geospatial.Api.Models;

public enum WaterBodyOrderBy
{
    AreaDesc
}

public static class WaterBodyOrderByParser
{
    public static bool TryParse(string? value, out WaterBodyOrderBy orderBy)
    {
        orderBy = WaterBodyOrderBy.AreaDesc;

        if (string.IsNullOrWhiteSpace(value))
        {
            return true;
        }

        return value switch
        {
            "areaDesc" => Set(WaterBodyOrderBy.AreaDesc, out orderBy),
            _ => false
        };
    }

    private static bool Set(WaterBodyOrderBy value, out WaterBodyOrderBy orderBy)
    {
        orderBy = value;
        return true;
    }
}

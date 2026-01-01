namespace Trimble.Geospatial.Api.Models;

public enum TileOrderBy
{
    TileId,
    PointCountDesc,
    WaterRatioDesc,
    HeightP99Desc,
    ReliefDesc,
    ComputedAtDesc
}

public static class TileOrderByParser
{
    public static bool TryParse(string? value, out TileOrderBy orderBy)
    {
        orderBy = TileOrderBy.TileId;

        if (string.IsNullOrWhiteSpace(value))
        {
            return true;
        }

        return value switch
        {
            "tileId" => Set(TileOrderBy.TileId, out orderBy),
            "pointCountDesc" => Set(TileOrderBy.PointCountDesc, out orderBy),
            "waterRatioDesc" => Set(TileOrderBy.WaterRatioDesc, out orderBy),
            "heightP99Desc" => Set(TileOrderBy.HeightP99Desc, out orderBy),
            "reliefDesc" => Set(TileOrderBy.ReliefDesc, out orderBy),
            "computedAtDesc" => Set(TileOrderBy.ComputedAtDesc, out orderBy),
            _ => false
        };
    }

    private static bool Set(TileOrderBy value, out TileOrderBy orderBy)
    {
        orderBy = value;
        return true;
    }
}

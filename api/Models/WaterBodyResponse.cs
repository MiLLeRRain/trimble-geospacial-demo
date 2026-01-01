namespace Trimble.Geospatial.Api.Models;

public sealed class WaterBodyResponse
{
    public long WaterBodyId { get; init; }
    public double AreaM2 { get; init; }
    public long CellCount { get; init; }
    public double MeanZ { get; init; }
    public double MinZ { get; init; }
    public double MaxZ { get; init; }
    public int BboxMinCellX { get; init; }
    public int BboxMinCellY { get; init; }
    public int BboxMaxCellX { get; init; }
    public int BboxMaxCellY { get; init; }
    public DateTimeOffset ComputedAt { get; init; }
}

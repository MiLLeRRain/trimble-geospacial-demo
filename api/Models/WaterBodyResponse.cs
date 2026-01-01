using System.ComponentModel.DataAnnotations;

namespace Trimble.Geospatial.Api.Models;

public sealed class WaterBodyResponse
{
    [Range(0, long.MaxValue)]
    public long WaterBodyId { get; init; }

    [Range(0, double.MaxValue)]
    public double AreaM2 { get; init; }

    [Range(0, long.MaxValue)]
    public long CellCount { get; init; }
    public double MeanZ { get; init; }
    public double MinZ { get; init; }
    public double MaxZ { get; init; }

    [Range(0, int.MaxValue)]
    public int BboxMinCellX { get; init; }

    [Range(0, int.MaxValue)]
    public int BboxMinCellY { get; init; }

    [Range(0, int.MaxValue)]
    public int BboxMaxCellX { get; init; }

    [Range(0, int.MaxValue)]
    public int BboxMaxCellY { get; init; }
    public DateTimeOffset ComputedAt { get; init; }
}

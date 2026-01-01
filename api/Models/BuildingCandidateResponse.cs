namespace Trimble.Geospatial.Api.Models;

public sealed class BuildingCandidateResponse
{
    public string BuildingCandidateId { get; init; } = string.Empty;
    public string TileId { get; init; } = string.Empty;
    public double HeightAboveGround { get; init; }
    public double HeightRange { get; init; }
    public long PointsUsed { get; init; }
    public long CellCount { get; init; }
    public double MinZ { get; init; }
    public double MeanZ { get; init; }
    public double MaxZ { get; init; }
    public DateTimeOffset ComputedAt { get; init; }
}

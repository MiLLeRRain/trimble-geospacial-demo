using System.ComponentModel.DataAnnotations;

namespace Trimble.Geospatial.Api.Models;

public sealed class BuildingCandidateResponse
{
    [Required]
    public string BuildingCandidateId { get; init; } = string.Empty;

    [Required]
    public string TileId { get; init; } = string.Empty;

    [Range(0, double.MaxValue)]
    public double HeightAboveGround { get; init; }

    [Range(0, double.MaxValue)]
    public double HeightRange { get; init; }

    [Range(0, long.MaxValue)]
    public long PointsUsed { get; init; }

    [Range(0, long.MaxValue)]
    public long CellCount { get; init; }
    public double MinZ { get; init; }
    public double MeanZ { get; init; }
    public double MaxZ { get; init; }
    public DateTimeOffset ComputedAt { get; init; }
}

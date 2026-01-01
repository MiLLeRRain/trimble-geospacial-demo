using System.ComponentModel.DataAnnotations;
using System.Text.Json.Serialization;

namespace Trimble.Geospatial.Api.Models;

public sealed class TileStatsResponse
{
    [Required]
    public string SiteId { get; init; } = string.Empty;

    [Required]
    public string TileId { get; init; } = string.Empty;

    [Range(0, long.MaxValue)]
    public long PointCount { get; init; }

    [JsonPropertyName("z_p50")]
    public double ZP50 { get; init; }

    [JsonPropertyName("z_p95")]
    public double ZP95 { get; init; }

    [JsonPropertyName("z_p99")]
    public double ZP99 { get; init; }

    [Range(0, 1)]
    public double WaterPointRatio { get; init; }
    public bool IsMostlyWater { get; init; }
    public DateTimeOffset ComputedAt { get; init; }
}

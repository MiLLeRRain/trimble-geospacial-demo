using System.ComponentModel.DataAnnotations;

namespace Trimble.Geospatial.Api.Models;

public sealed class PipelineRunStatusResponse
{
    [Required]
    public string SiteId { get; init; } = string.Empty;

    [Required]
    public string RunId { get; init; } = string.Empty;

    [Required]
    public string Status { get; init; } = string.Empty;
    public DateTimeOffset StartedAt { get; init; }
    public DateTimeOffset? FinishedAt { get; init; }
    public string? ErrorMessage { get; init; }
    public DateTimeOffset? ProducedSnapshotAt { get; init; }
}

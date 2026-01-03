using System.ComponentModel.DataAnnotations;
using System.Text.Json.Serialization;

namespace Trimble.Geospatial.Api.Models;

public sealed class UploadSessionRequest
{
    [Required]
    [StringLength(128, MinimumLength = 1)]
    public string SiteId { get; init; } = string.Empty;

    [Required]
    [StringLength(256, MinimumLength = 1)]
    public string FileName { get; init; } = string.Empty;

    [Required]
    [StringLength(128, MinimumLength = 1)]
    public string ContentType { get; init; } = string.Empty;

    [Range(1, long.MaxValue)]
    public long ContentLength { get; init; }
}

public sealed class UploadSessionResponse
{
    [JsonPropertyName("jobId")]
    public string JobId { get; init; } = string.Empty;

    [JsonPropertyName("uploadId")]
    public string UploadId { get; init; } = string.Empty;

    [JsonPropertyName("landing_path")]
    public string LandingPath { get; init; } = string.Empty;

    [JsonPropertyName("uploadUrl")]
    public string UploadUrl { get; init; } = string.Empty;

    [JsonPropertyName("expiresAtUtc")]
    public DateTimeOffset ExpiresAtUtc { get; init; }
}

public sealed class UploadCompleteRequest
{
    [JsonPropertyName("etag")]
    public string? ETag { get; init; }

    [JsonPropertyName("sizeBytes")]
    public long? SizeBytes { get; init; }
}

public sealed class UploadStatusResponse
{
    [JsonPropertyName("jobId")]
    public string JobId { get; init; } = string.Empty;

    [JsonPropertyName("status")]
    public string Status { get; init; } = string.Empty;

    [JsonPropertyName("landing_path")]
    public string LandingPath { get; init; } = string.Empty;

    [JsonPropertyName("siteId")]
    public string SiteId { get; init; } = string.Empty;

    [JsonPropertyName("ingestRunId")]
    public string IngestRunId { get; init; } = string.Empty;
}

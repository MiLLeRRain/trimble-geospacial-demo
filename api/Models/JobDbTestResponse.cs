namespace Trimble.Geospatial.Api.Models;

public sealed record JobDbTestResponse(
    string CurrentDb,
    string LoginName,
    JobDbTestJob InsertedJob);

public sealed record JobDbTestJob(
    string JobId,
    string SiteId,
    string Status,
    string TargetIngestRunId,
    DateTime CreatedAtUtc);

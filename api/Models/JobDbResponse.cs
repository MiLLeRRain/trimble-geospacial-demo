namespace Trimble.Geospatial.Api.Models;

public sealed record JobDbResponse(
    string CurrentDb,
    string LoginName,
    JobDbJob InsertedJob);

public sealed record JobDbJob(
    string JobId,
    string SiteId,
    string Status,
    string TargetIngestRunId,
    DateTime CreatedAtUtc);

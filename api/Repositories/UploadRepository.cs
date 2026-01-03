using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using Trimble.Geospatial.Api.Options;

namespace Trimble.Geospatial.Api.Repositories;

public sealed class UploadRepository
{
    private readonly JobDbOptions _options;
    private readonly ILogger<UploadRepository> _logger;

    public UploadRepository(IOptions<JobDbOptions> options, ILogger<UploadRepository> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public async Task<UploadSessionRecord?> GetByIdempotencyKeyAsync(string idempotencyKey, CancellationToken cancellationToken)
    {
        var connectionString = _options.GetConnectionString();
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        const string sql = @"
SELECT TOP 1
    j.JobId,
    j.SiteId,
    j.Status,
    j.TargetIngestRunId,
    j.LandingPath,
    j.IdempotencyKey,
    u.UploadId,
    u.BlobUri,
    u.UploadStatus
FROM dbo.Jobs j
INNER JOIN dbo.Uploads u ON u.JobId = j.JobId
WHERE j.IdempotencyKey = @IdempotencyKey
ORDER BY u.CreatedAtUtc DESC;";

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        cmd.Parameters.Add(new SqlParameter("@IdempotencyKey", SqlDbType.NVarChar, 128) { Value = idempotencyKey });

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return MapSession(reader);
    }

    public async Task<UploadSessionRecord> InsertUploadSessionAsync(
        UploadSessionInsert insert,
        CancellationToken cancellationToken)
    {
        var connectionString = _options.GetConnectionString();
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var tx = (SqlTransaction)await connection.BeginTransactionAsync();

        try
        {
            await InsertJobAsync(connection, tx, insert, cancellationToken);
            await InsertUploadAsync(connection, tx, insert, cancellationToken);
            await tx.CommitAsync(cancellationToken);
        }
        catch (SqlException ex) when (ex.Number is 2601 or 2627)
        {
            _logger.LogWarning(ex, "IdempotencyKey conflict, retrieving existing session.");
            await tx.RollbackAsync(cancellationToken);
            var existing = await GetByIdempotencyKeyAsync(insert.IdempotencyKey, cancellationToken);
            if (existing is not null)
            {
                return existing;
            }

            throw;
        }

        return new UploadSessionRecord(
            insert.JobId,
            insert.UploadId,
            insert.SiteId,
            "CREATED",
            insert.TargetIngestRunId,
            insert.LandingPath,
            insert.BlobUri,
            "URL_ISSUED");
    }

    public async Task<UploadWithJob?> GetUploadWithJobAsync(string uploadId, CancellationToken cancellationToken)
    {
        var connectionString = _options.GetConnectionString();
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        const string sql = @"
SELECT
    u.UploadId,
    u.JobId,
    u.FileName,
    u.ContentType,
    u.SizeBytes,
    u.BlobUri,
    u.BlobETag,
    u.UploadStatus,
    u.CompletedAtUtc,
    j.SiteId,
    j.Status,
    j.TargetIngestRunId,
    j.LandingPath
FROM dbo.Uploads u
INNER JOIN dbo.Jobs j ON u.JobId = j.JobId
WHERE u.UploadId = @UploadId;";

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        cmd.Parameters.Add(new SqlParameter("@UploadId", SqlDbType.NVarChar, 64) { Value = uploadId });

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return MapUploadWithJob(reader);
    }

    public async Task<UploadCompletionResult?> CompleteUploadAsync(
        string uploadId,
        string? blobEtag,
        long? sizeBytes,
        CancellationToken cancellationToken)
    {
        var connectionString = _options.GetConnectionString();
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var tx = (SqlTransaction)await connection.BeginTransactionAsync();

        var current = await GetUploadWithJobForUpdateAsync(connection, tx, uploadId, cancellationToken);
        if (current is null)
        {
            await tx.RollbackAsync(cancellationToken);
            return null;
        }

        if (string.Equals(current.UploadStatus, "COMPLETED", StringComparison.OrdinalIgnoreCase))
        {
            await tx.CommitAsync(cancellationToken);
            return new UploadCompletionResult(current.JobId, current.SiteId, current.JobStatus, current.LandingPath, current.TargetIngestRunId);
        }

        await UpdateUploadAsync(connection, tx, current.UploadId, blobEtag, sizeBytes, cancellationToken);
        var newJobStatus = await UpdateJobStatusAsync(connection, tx, current.JobId, cancellationToken)
            ?? current.JobStatus;

        await tx.CommitAsync(cancellationToken);

        return new UploadCompletionResult(current.JobId, current.SiteId, newJobStatus, current.LandingPath, current.TargetIngestRunId);
    }

    private static UploadSessionRecord MapSession(SqlDataReader reader)
    {
        var landingPath = reader.IsDBNull(4) ? null : reader.GetString(4);
        var blobUri = reader.IsDBNull(7) ? null : reader.GetString(7);

        return new UploadSessionRecord(
            reader.GetString(0),
            reader.GetString(6),
            reader.GetString(1),
            reader.GetString(2),
            reader.GetString(3),
            landingPath ?? blobUri ?? string.Empty,
            blobUri ?? string.Empty,
            reader.GetString(8));
    }

    private static UploadWithJob MapUploadWithJob(SqlDataReader reader)
    {
        return new UploadWithJob(
            reader.GetString(0),
            reader.GetString(1),
            reader.IsDBNull(2) ? null : reader.GetString(2),
            reader.IsDBNull(3) ? null : reader.GetString(3),
            reader.IsDBNull(4) ? null : reader.GetInt64(4),
            reader.IsDBNull(5) ? string.Empty : reader.GetString(5),
            reader.IsDBNull(6) ? null : reader.GetString(6),
            reader.GetString(7),
            reader.IsDBNull(8) ? (DateTime?)null : reader.GetDateTime(8),
            reader.GetString(9),
            reader.GetString(10),
            reader.GetString(11),
            reader.IsDBNull(12) ? string.Empty : reader.GetString(12));
    }

    private async Task InsertJobAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        UploadSessionInsert insert,
        CancellationToken cancellationToken)
    {
        const string sql = @"
INSERT INTO dbo.Jobs (JobId, SiteId, Status, TargetIngestRunId, LandingPath, IdempotencyKey, CorrelationId)
VALUES (@JobId, @SiteId, @Status, @TargetIngestRunId, @LandingPath, @IdempotencyKey, @CorrelationId);";

        await using var cmd = connection.CreateCommand();
        cmd.Transaction = transaction;
        cmd.CommandText = sql;
        cmd.Parameters.Add(new SqlParameter("@JobId", SqlDbType.NVarChar, 64) { Value = insert.JobId });
        cmd.Parameters.Add(new SqlParameter("@SiteId", SqlDbType.NVarChar, 128) { Value = insert.SiteId });
        cmd.Parameters.Add(new SqlParameter("@Status", SqlDbType.NVarChar, 32) { Value = "CREATED" });
        cmd.Parameters.Add(new SqlParameter("@TargetIngestRunId", SqlDbType.NVarChar, 64) { Value = insert.TargetIngestRunId });
        cmd.Parameters.Add(new SqlParameter("@LandingPath", SqlDbType.NVarChar, 1024) { Value = insert.LandingPath });
        cmd.Parameters.Add(new SqlParameter("@IdempotencyKey", SqlDbType.NVarChar, 128) { Value = insert.IdempotencyKey });
        cmd.Parameters.Add(new SqlParameter("@CorrelationId", SqlDbType.NVarChar, 64) { Value = insert.CorrelationId });

        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task InsertUploadAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        UploadSessionInsert insert,
        CancellationToken cancellationToken)
    {
        const string sql = @"
INSERT INTO dbo.Uploads (UploadId, JobId, FileName, ContentType, SizeBytes, BlobUri, UploadStatus)
VALUES (@UploadId, @JobId, @FileName, @ContentType, @SizeBytes, @BlobUri, @UploadStatus);";

        await using var cmd = connection.CreateCommand();
        cmd.Transaction = transaction;
        cmd.CommandText = sql;

        cmd.Parameters.Add(new SqlParameter("@UploadId", SqlDbType.NVarChar, 64) { Value = insert.UploadId });
        cmd.Parameters.Add(new SqlParameter("@JobId", SqlDbType.NVarChar, 64) { Value = insert.JobId });
        cmd.Parameters.Add(new SqlParameter("@FileName", SqlDbType.NVarChar, 256) { Value = insert.FileName });
        cmd.Parameters.Add(new SqlParameter("@ContentType", SqlDbType.NVarChar, 128) { Value = insert.ContentType });
        cmd.Parameters.Add(new SqlParameter("@SizeBytes", SqlDbType.BigInt) { Value = insert.ContentLength });
        cmd.Parameters.Add(new SqlParameter("@BlobUri", SqlDbType.NVarChar, 1024) { Value = insert.BlobUri });
        cmd.Parameters.Add(new SqlParameter("@UploadStatus", SqlDbType.NVarChar, 32) { Value = "URL_ISSUED" });

        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task<UploadWithJob?> GetUploadWithJobForUpdateAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        string uploadId,
        CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT
    u.UploadId,
    u.JobId,
    u.FileName,
    u.ContentType,
    u.SizeBytes,
    u.BlobUri,
    u.BlobETag,
    u.UploadStatus,
    u.CompletedAtUtc,
    j.SiteId,
    j.Status,
    j.TargetIngestRunId,
    j.LandingPath
FROM dbo.Uploads u WITH (UPDLOCK, ROWLOCK)
INNER JOIN dbo.Jobs j WITH (UPDLOCK, ROWLOCK) ON u.JobId = j.JobId
WHERE u.UploadId = @UploadId;";

        await using var cmd = connection.CreateCommand();
        cmd.Transaction = transaction;
        cmd.CommandText = sql;
        cmd.Parameters.Add(new SqlParameter("@UploadId", SqlDbType.NVarChar, 64) { Value = uploadId });

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return MapUploadWithJob(reader);
    }

    private static async Task UpdateUploadAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        string uploadId,
        string? blobEtag,
        long? sizeBytes,
        CancellationToken cancellationToken)
    {
        const string sql = @"
UPDATE dbo.Uploads
SET UploadStatus = 'COMPLETED',
    CompletedAtUtc = CASE WHEN CompletedAtUtc IS NULL THEN SYSUTCDATETIME() ELSE CompletedAtUtc END,
    BlobETag = COALESCE(@BlobETag, BlobETag),
    SizeBytes = COALESCE(@SizeBytes, SizeBytes)
WHERE UploadId = @UploadId;";

        await using var cmd = connection.CreateCommand();
        cmd.Transaction = transaction;
        cmd.CommandText = sql;
        cmd.Parameters.Add(new SqlParameter("@BlobETag", SqlDbType.NVarChar, 128) { Value = (object?)blobEtag ?? DBNull.Value });
        cmd.Parameters.Add(new SqlParameter("@SizeBytes", SqlDbType.BigInt) { Value = (object?)sizeBytes ?? DBNull.Value });
        cmd.Parameters.Add(new SqlParameter("@UploadId", SqlDbType.NVarChar, 64) { Value = uploadId });

        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<string?> UpdateJobStatusAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        string jobId,
        CancellationToken cancellationToken)
    {
        const string sql = @"
UPDATE dbo.Jobs
SET Status = 'QUEUED',
    UpdatedAtUtc = SYSUTCDATETIME()
WHERE JobId = @JobId AND Status = 'CREATED';";

        await using var cmd = connection.CreateCommand();
        cmd.Transaction = transaction;
        cmd.CommandText = sql;
        cmd.Parameters.Add(new SqlParameter("@JobId", SqlDbType.NVarChar, 64) { Value = jobId });

        var rows = await cmd.ExecuteNonQueryAsync(cancellationToken);
        return rows > 0 ? "QUEUED" : null;
    }
}

public sealed record UploadSessionInsert(
    string JobId,
    string UploadId,
    string SiteId,
    string FileName,
    string ContentType,
    long ContentLength,
    string BlobUri,
    string LandingPath,
    string IdempotencyKey,
    string TargetIngestRunId,
    string CorrelationId);

public sealed record UploadSessionRecord(
    string JobId,
    string UploadId,
    string SiteId,
    string JobStatus,
    string TargetIngestRunId,
    string LandingPath,
    string BlobUri,
    string UploadStatus);

public sealed record UploadWithJob(
    string UploadId,
    string JobId,
    string? FileName,
    string? ContentType,
    long? SizeBytes,
    string BlobUri,
    string? BlobETag,
    string UploadStatus,
    DateTime? CompletedAtUtc,
    string SiteId,
    string JobStatus,
    string TargetIngestRunId,
    string LandingPath);

public sealed record UploadCompletionResult(
    string JobId,
    string SiteId,
    string JobStatus,
    string LandingPath,
    string TargetIngestRunId);

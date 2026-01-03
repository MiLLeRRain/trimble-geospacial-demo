using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using Trimble.Geospatial.Demo.Functions.Options;

namespace Trimble.Geospatial.Demo.Functions.Repositories;

public sealed class JobRepository
{
    private readonly JobDbOptions _options;

    public JobRepository(IOptions<JobDbOptions> options)
    {
        _options = options.Value;
    }

    public async Task<string?> GetJobStatusAsync(string jobId, CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT Status FROM dbo.Jobs WHERE JobId = @JobId;";
        cmd.Parameters.Add(new SqlParameter("@JobId", jobId));

        var result = await cmd.ExecuteScalarAsync(cancellationToken);
        return result as string;
    }

    public async Task<int> TryMarkDbxTriggeredAsync(string jobId, string landingPath, string ingestRunId, CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
UPDATE dbo.Jobs
SET Status = @Status,
    LandingPath = @LandingPath,
    TargetIngestRunId = @TargetIngestRunId,
    UpdatedAtUtc = SYSUTCDATETIME()
WHERE JobId = @JobId
  AND (Status IS NULL OR Status NOT IN ('DBX_TRIGGERED','PROCESSING','SUCCEEDED','FAILED'));";

        cmd.Parameters.Add(new SqlParameter("@JobId", jobId));
        cmd.Parameters.Add(new SqlParameter("@Status", "DBX_TRIGGERED"));
        cmd.Parameters.Add(new SqlParameter("@LandingPath", landingPath));
        cmd.Parameters.Add(new SqlParameter("@TargetIngestRunId", ingestRunId));

        return await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<int> SetProcessingAsync(string jobId, long runId, CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
UPDATE dbo.Jobs
SET DatabricksRunId = @RunId,
    Status = @Status,
    UpdatedAtUtc = SYSUTCDATETIME()
WHERE JobId = @JobId;";

        cmd.Parameters.Add(new SqlParameter("@JobId", jobId));
        cmd.Parameters.Add(new SqlParameter("@RunId", runId));
        cmd.Parameters.Add(new SqlParameter("@Status", "PROCESSING"));

        return await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<int> SetFailedAsync(string jobId, string errorCode, string? errorMessage, CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
UPDATE dbo.Jobs
SET Status = @Status,
    ErrorCode = @ErrorCode,
    ErrorMessage = @ErrorMessage,
    UpdatedAtUtc = SYSUTCDATETIME()
WHERE JobId = @JobId;";

        cmd.Parameters.Add(new SqlParameter("@JobId", jobId));
        cmd.Parameters.Add(new SqlParameter("@Status", "FAILED"));
        cmd.Parameters.Add(new SqlParameter("@ErrorCode", Truncate(errorCode, 64)));
        cmd.Parameters.Add(new SqlParameter("@ErrorMessage", ToDbValue(Truncate(errorMessage, 2048))));

        return await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<int> UpdateFromWebhookByRunIdAsync(long runId, string status, string? errorCode, string? errorMessage, CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
UPDATE dbo.Jobs
SET Status = @Status,
    ErrorCode = @ErrorCode,
    ErrorMessage = @ErrorMessage,
    UpdatedAtUtc = SYSUTCDATETIME()
WHERE DatabricksRunId = @RunId;";

        cmd.Parameters.Add(new SqlParameter("@RunId", runId));
        cmd.Parameters.Add(new SqlParameter("@Status", status));
        cmd.Parameters.Add(new SqlParameter("@ErrorCode", ToDbValue(Truncate(errorCode, 64))));
        cmd.Parameters.Add(new SqlParameter("@ErrorMessage", ToDbValue(Truncate(errorMessage, 2048))));

        return await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<int> UpdateFromWebhookByJobIdAsync(string jobId, string status, string? errorCode, string? errorMessage, CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
UPDATE dbo.Jobs
SET Status = @Status,
    ErrorCode = @ErrorCode,
    ErrorMessage = @ErrorMessage,
    UpdatedAtUtc = SYSUTCDATETIME()
WHERE JobId = @JobId;";

        cmd.Parameters.Add(new SqlParameter("@JobId", jobId));
        cmd.Parameters.Add(new SqlParameter("@Status", status));
        cmd.Parameters.Add(new SqlParameter("@ErrorCode", ToDbValue(Truncate(errorCode, 64))));
        cmd.Parameters.Add(new SqlParameter("@ErrorMessage", ToDbValue(Truncate(errorMessage, 2048))));

        return await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task<SqlConnection> OpenConnectionAsync(CancellationToken cancellationToken)
    {
        var connection = new SqlConnection(_options.GetConnectionString());
        await connection.OpenAsync(cancellationToken);
        return connection;
    }

    private static object ToDbValue(string? value) => value is null ? DBNull.Value : value;

    private static string? Truncate(string? value, int maxLength)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        return value.Length <= maxLength ? value : value[..maxLength];
    }
}

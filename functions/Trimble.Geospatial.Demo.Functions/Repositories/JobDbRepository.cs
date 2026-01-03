using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using Trimble.Geospatial.Demo.Functions.Options;

namespace Trimble.Geospatial.Demo.Functions.Repositories;

public sealed class JobDbRepository
{
    private readonly JobDbOptions _options;

    public JobDbRepository(IOptions<JobDbOptions> options)
    {
        _options = options.Value;
    }

    public async Task<(string currentDb, string loginName)> GetIdentityAsync(CancellationToken cancellationToken)
    {
        await using var connection = new SqlConnection(_options.GetConnectionString());
        await connection.OpenAsync(cancellationToken);

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT DB_NAME() AS CurrentDb, SUSER_SNAME() AS LoginName;";

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            throw new InvalidOperationException("Failed to query identity.");
        }

        return (reader.GetString(0), reader.GetString(1));
    }

    public async Task<int> UpdateJobStatusAsync(string jobId, string newStatus, CancellationToken cancellationToken)
    {
        await using var connection = new SqlConnection(_options.GetConnectionString());
        await connection.OpenAsync(cancellationToken);

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
+UPDATE dbo.Jobs
+SET Status = @Status,
+    UpdatedAtUtc = SYSUTCDATETIME()
+WHERE JobId = @JobId;";

        cmd.Parameters.AddWithValue("@JobId", jobId);
        cmd.Parameters.AddWithValue("@Status", newStatus);

        return await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<string?> GetLatestJobIdAsync(CancellationToken cancellationToken)
    {
        await using var connection = new SqlConnection(_options.GetConnectionString());
        await connection.OpenAsync(cancellationToken);

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT TOP 1 JobId FROM dbo.Jobs ORDER BY CreatedAtUtc DESC;";

        var result = await cmd.ExecuteScalarAsync(cancellationToken);
        return result as string;
    }
}

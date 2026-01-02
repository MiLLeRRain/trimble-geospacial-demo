using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using Trimble.Geospatial.Api.Models;
using Trimble.Geospatial.Api.Options;

namespace Trimble.Geospatial.Api.Repositories;

public sealed class JobDbTestRepository
{
    private readonly JobDbOptions _options;

    public JobDbTestRepository(IOptions<JobDbOptions> options)
    {
        _options = options.Value;
    }

    public async Task<JobDbTestResponse> InsertAndSelectAsync(CancellationToken cancellationToken)
    {
        var connectionString = _options.GetConnectionString();

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        // Helpful for verifying that Managed Identity / AAD auth is actually in effect.
        string currentDb;
        string loginName;
        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "SELECT DB_NAME() AS CurrentDb, SUSER_SNAME() AS LoginName;";
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                throw new InvalidOperationException("Failed to query DB identity.");
            }

            currentDb = reader.GetString(0);
            loginName = reader.GetString(1);
        }

        var jobId = Guid.NewGuid().ToString("N");
        var siteId = "_test";
        var status = "CREATED";
        var targetIngestRunId = Guid.NewGuid().ToString("N");

        await using (var insert = connection.CreateCommand())
        {
            insert.CommandText = @"
INSERT INTO dbo.Jobs (JobId, SiteId, Status, TargetIngestRunId)
VALUES (@JobId, @SiteId, @Status, @TargetIngestRunId);";

            insert.Parameters.AddWithValue("@JobId", jobId);
            insert.Parameters.AddWithValue("@SiteId", siteId);
            insert.Parameters.AddWithValue("@Status", status);
            insert.Parameters.AddWithValue("@TargetIngestRunId", targetIngestRunId);

            await insert.ExecuteNonQueryAsync(cancellationToken);
        }

        await using var select = connection.CreateCommand();
        select.CommandText = @"
SELECT TOP 1 JobId, SiteId, Status, TargetIngestRunId, CreatedAtUtc
FROM dbo.Jobs
WHERE JobId = @JobId;";
        select.Parameters.AddWithValue("@JobId", jobId);

        await using var selectReader = await select.ExecuteReaderAsync(cancellationToken);
        if (!await selectReader.ReadAsync(cancellationToken))
        {
            throw new InvalidOperationException("Insert succeeded but select did not return a row.");
        }

        var inserted = new JobDbTestJob(
            JobId: selectReader.GetString(0),
            SiteId: selectReader.GetString(1),
            Status: selectReader.GetString(2),
            TargetIngestRunId: selectReader.GetString(3),
            CreatedAtUtc: selectReader.GetDateTime(4));

        return new JobDbTestResponse(
            CurrentDb: currentDb,
            LoginName: loginName,
            InsertedJob: inserted);
    }
}

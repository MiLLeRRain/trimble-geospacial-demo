using Trimble.Geospatial.Api.Models;
using Trimble.Geospatial.Api.Services;

namespace Trimble.Geospatial.Api.Repositories;

public sealed class PipelineRunRepository
{
    private const string LatestQueryName = "GetLatestPipelineRun";
    private const string ByIdQueryName = "GetPipelineRunById";

    private const string LatestSql = """
        SELECT
          to_json(named_struct(
            'siteId', siteId,
            'runId', runId,
            'status', status,
            'startedAt', startedAt,
            'finishedAt', finishedAt,
            'errorMessage', errorMessage,
            'producedSnapshotAt', producedSnapshotEnd
          )) AS payload
        FROM main.demo.pipeline_runs
        WHERE siteId = :siteId
        ORDER BY createdAt DESC
        LIMIT 1
        """;

    private const string ByIdSql = """
        SELECT
          to_json(named_struct(
            'siteId', siteId,
            'runId', runId,
            'status', status,
            'startedAt', startedAt,
            'finishedAt', finishedAt,
            'errorMessage', errorMessage,
            'producedSnapshotAt', producedSnapshotEnd
          )) AS payload
        FROM main.demo.pipeline_runs
        WHERE siteId = :siteId
          AND runId = :runId
        LIMIT 1
        """;

    private readonly DatabricksSqlQueryExecutor _queryExecutor;

    public PipelineRunRepository(DatabricksSqlQueryExecutor queryExecutor)
    {
        _queryExecutor = queryExecutor;
    }

    public Task<PipelineRunStatusResponse?> GetLatestAsync(string siteId, CancellationToken cancellationToken)
    {
        var parameters = new[]
        {
            DatabricksSqlParameter.String("siteId", siteId)
        };

        return _queryExecutor.QuerySingleAsync<PipelineRunStatusResponse>(LatestQueryName, LatestSql, parameters, cancellationToken);
    }

    public Task<PipelineRunStatusResponse?> GetByIdAsync(string siteId, string runId, CancellationToken cancellationToken)
    {
        var parameters = new[]
        {
            DatabricksSqlParameter.String("siteId", siteId),
            DatabricksSqlParameter.String("runId", runId)
        };

        return _queryExecutor.QuerySingleAsync<PipelineRunStatusResponse>(ByIdQueryName, ByIdSql, parameters, cancellationToken);
    }
}

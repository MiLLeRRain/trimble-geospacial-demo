using System.Text;
using Trimble.Geospatial.Api.Models;
using Trimble.Geospatial.Api.Services;

namespace Trimble.Geospatial.Api.Repositories;

public sealed class BuildingCandidateRepository
{
    private const string ListQueryName = "GetBuildingCandidates";
    private const string DetailQueryName = "GetBuildingCandidateById";

    private const string SelectSql = """
        SELECT
          to_json(named_struct(
            'buildingCandidateId', buildingCandidateId,
            'tileId', tileId,
            'heightAboveGround', heightAboveGround,
            'heightRange', heightRange,
            'pointsUsed', pointsUsed,
            'cellCount', cellCount,
            'minZ', minZ,
            'meanZ', meanZ,
            'maxZ', maxZ,
            'computedAt', computedAt
          )) AS payload
        FROM main.demo.features_building_candidates_v2
        WHERE siteId = :siteId
        """;

    private readonly DatabricksSqlQueryExecutor _queryExecutor;

    public BuildingCandidateRepository(DatabricksSqlQueryExecutor queryExecutor)
    {
        _queryExecutor = queryExecutor;
    }

    public Task<IReadOnlyList<BuildingCandidateResponse>> GetListAsync(
        string siteId,
        string? tileId,
        double? minHeight,
        BuildingCandidateOrderBy orderBy,
        int limit,
        CancellationToken cancellationToken)
    {
        var sql = BuildListSql(tileId, minHeight, orderBy);
        var parameters = BuildListParameters(siteId, tileId, minHeight, limit);

        return _queryExecutor.QueryListAsync<BuildingCandidateResponse>(ListQueryName, sql, parameters, cancellationToken);
    }

    public Task<BuildingCandidateResponse?> GetByIdAsync(
        string siteId,
        string tileId,
        string candidateId,
        CancellationToken cancellationToken)
    {
        var sql = BuildDetailSql();
        var parameters = new[]
        {
            DatabricksSqlParameter.String("siteId", siteId),
            DatabricksSqlParameter.String("tileId", tileId),
            DatabricksSqlParameter.String("candidateId", candidateId)
        };

        return _queryExecutor.QuerySingleAsync<BuildingCandidateResponse>(DetailQueryName, sql, parameters, cancellationToken);
    }

    private static string BuildListSql(string? tileId, double? minHeight, BuildingCandidateOrderBy orderBy)
    {
        var builder = new StringBuilder(SelectSql);

        builder.AppendLine();

        if (!string.IsNullOrWhiteSpace(tileId))
        {
            builder.AppendLine("  AND tileId = :tileId");
        }

        if (minHeight.HasValue)
        {
            builder.AppendLine("  AND heightAboveGround >= :minHeight");
        }

        builder.AppendLine(BuildOrderBy(orderBy));
        builder.AppendLine("LIMIT :limit");

        return builder.ToString();
    }

    private static string BuildDetailSql()
    {
        var builder = new StringBuilder(SelectSql);
        builder.AppendLine("  AND tileId = :tileId");
        builder.AppendLine("  AND buildingCandidateId = :candidateId");
        builder.AppendLine("LIMIT 1");
        return builder.ToString();
    }

    private static string BuildOrderBy(BuildingCandidateOrderBy orderBy)
    {
        return orderBy switch
        {
            BuildingCandidateOrderBy.HeightDesc => "ORDER BY heightAboveGround DESC",
            BuildingCandidateOrderBy.HeightRangeDesc => "ORDER BY heightRange DESC",
            _ => "ORDER BY heightAboveGround DESC"
        };
    }

    private static IReadOnlyCollection<DatabricksSqlParameter> BuildListParameters(string siteId, string? tileId, double? minHeight, int limit)
    {
        var parameters = new List<DatabricksSqlParameter>
        {
            DatabricksSqlParameter.String("siteId", siteId),
            DatabricksSqlParameter.Int("limit", limit)
        };

        if (!string.IsNullOrWhiteSpace(tileId))
        {
            parameters.Add(DatabricksSqlParameter.String("tileId", tileId));
        }

        if (minHeight.HasValue)
        {
            parameters.Add(DatabricksSqlParameter.Double("minHeight", minHeight.Value));
        }

        return parameters;
    }
}

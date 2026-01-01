using System.Text;
using Trimble.Geospatial.Api.Models;
using Trimble.Geospatial.Api.Services;

namespace Trimble.Geospatial.Api.Repositories;

public sealed class WaterBodyRepository
{
    private const string ListQueryName = "GetWaterBodies";
    private const string DetailQueryName = "GetWaterBodyById";

    private const string SelectSql = """
        SELECT
          to_json(named_struct(
            'waterBodyId', waterBodyId,
            'areaM2', areaM2,
            'cellCount', cellCount,
            'meanZ', meanZ,
            'minZ', minZ,
            'maxZ', maxZ,
            'bboxMinCellX', bboxMinCellX,
            'bboxMinCellY', bboxMinCellY,
            'bboxMaxCellX', bboxMaxCellX,
            'bboxMaxCellY', bboxMaxCellY,
            'computedAt', computedAt
          )) AS payload
        FROM main.demo.features_water_bodies_v2
        WHERE siteId = :siteId
        """;

    private readonly DatabricksSqlQueryExecutor _queryExecutor;

    public WaterBodyRepository(DatabricksSqlQueryExecutor queryExecutor)
    {
        _queryExecutor = queryExecutor;
    }

    public Task<IReadOnlyList<WaterBodyResponse>> GetListAsync(
        string siteId,
        double? minAreaM2,
        WaterBodyOrderBy orderBy,
        int limit,
        CancellationToken cancellationToken)
    {
        var sql = BuildListSql(minAreaM2, orderBy);
        var parameters = BuildListParameters(siteId, minAreaM2, limit);

        return _queryExecutor.QueryListAsync<WaterBodyResponse>(ListQueryName, sql, parameters, cancellationToken);
    }

    public Task<WaterBodyResponse?> GetByIdAsync(string siteId, long waterBodyId, CancellationToken cancellationToken)
    {
        var sql = BuildDetailSql();
        var parameters = new[]
        {
            DatabricksSqlParameter.String("siteId", siteId),
            DatabricksSqlParameter.Long("waterBodyId", waterBodyId)
        };

        return _queryExecutor.QuerySingleAsync<WaterBodyResponse>(DetailQueryName, sql, parameters, cancellationToken);
    }

    private static string BuildListSql(double? minAreaM2, WaterBodyOrderBy orderBy)
    {
        var builder = new StringBuilder(SelectSql);

        builder.AppendLine();

        if (minAreaM2.HasValue)
        {
            builder.AppendLine("  AND areaM2 >= :minAreaM2");
        }

        builder.AppendLine(BuildOrderBy(orderBy));
        builder.AppendLine("LIMIT :limit");

        return builder.ToString();
    }

    private static string BuildDetailSql()
    {
        var builder = new StringBuilder(SelectSql);
        builder.AppendLine("  AND waterBodyId = :waterBodyId");
        builder.AppendLine("LIMIT 1");
        return builder.ToString();
    }

    private static string BuildOrderBy(WaterBodyOrderBy orderBy)
    {
        return orderBy switch
        {
            WaterBodyOrderBy.AreaDesc => "ORDER BY areaM2 DESC",
            _ => "ORDER BY areaM2 DESC"
        };
    }

    private static IReadOnlyCollection<DatabricksSqlParameter> BuildListParameters(string siteId, double? minAreaM2, int limit)
    {
        var parameters = new List<DatabricksSqlParameter>
        {
            DatabricksSqlParameter.String("siteId", siteId),
            DatabricksSqlParameter.Int("limit", limit)
        };

        if (minAreaM2.HasValue)
        {
            parameters.Add(DatabricksSqlParameter.Double("minAreaM2", minAreaM2.Value));
        }

        return parameters;
    }
}

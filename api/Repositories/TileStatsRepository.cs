using System.Text;
using Trimble.Geospatial.Api.Models;
using Trimble.Geospatial.Api.Services;

namespace Trimble.Geospatial.Api.Repositories;

public sealed class TileStatsRepository
{
    private const string QueryName = "GetTileStats";

    private const string SelectSql = """
        SELECT
          to_json(named_struct(
            'siteId', siteId,
            'tileId', tileId,
            'pointCount', pointCount,
            'z_p50', z_p50,
            'z_p95', z_p95,
            'z_p99', z_p99,
            'waterPointRatio', waterPointRatio,
            'isMostlyWater', isMostlyWater,
            'computedAt', computedAt
          )) AS payload
        FROM main.demo.tile_stats_v2
        WHERE siteId = :siteId
        """;

    private readonly DatabricksSqlQueryExecutor _queryExecutor;

    public TileStatsRepository(DatabricksSqlQueryExecutor queryExecutor)
    {
        _queryExecutor = queryExecutor;
    }

    public Task<IReadOnlyList<TileStatsResponse>> GetTilesAsync(
        string siteId,
        bool skipMostlyWater,
        long? minPointCount,
        TileOrderBy orderBy,
        int limit,
        int offset,
        CancellationToken cancellationToken)
    {
        var sql = BuildSql(skipMostlyWater, minPointCount, orderBy);
        var parameters = BuildParameters(siteId, minPointCount, limit, offset);

        return _queryExecutor.QueryListAsync<TileStatsResponse>(QueryName, sql, parameters, cancellationToken);
    }

    private static string BuildSql(bool skipMostlyWater, long? minPointCount, TileOrderBy orderBy)
    {
        var builder = new StringBuilder(SelectSql);

        builder.AppendLine();

        if (skipMostlyWater)
        {
            builder.AppendLine("  AND isMostlyWater = false");
        }

        if (minPointCount.HasValue)
        {
            builder.AppendLine("  AND pointCount >= :minPointCount");
        }

        builder.AppendLine(BuildOrderBy(orderBy));
        builder.AppendLine("LIMIT :limit");
        builder.AppendLine("OFFSET :offset");

        return builder.ToString();
    }

    private static string BuildOrderBy(TileOrderBy orderBy)
    {
        return orderBy switch
        {
            TileOrderBy.TileId => "ORDER BY tileId ASC",
            TileOrderBy.PointCountDesc => "ORDER BY pointCount DESC",
            TileOrderBy.WaterRatioDesc => "ORDER BY waterPointRatio DESC",
            TileOrderBy.HeightP99Desc => "ORDER BY z_p99 DESC",
            TileOrderBy.ReliefDesc => "ORDER BY (z_p95 - z_p50) DESC",
            TileOrderBy.ComputedAtDesc => "ORDER BY computedAt DESC",
            _ => "ORDER BY tileId ASC"
        };
    }

    private static IReadOnlyCollection<DatabricksSqlParameter> BuildParameters(string siteId, long? minPointCount, int limit, int offset)
    {
        var parameters = new List<DatabricksSqlParameter>
        {
            DatabricksSqlParameter.String("siteId", siteId),
            DatabricksSqlParameter.Int("limit", limit),
            DatabricksSqlParameter.Int("offset", offset)
        };

        if (minPointCount.HasValue)
        {
            parameters.Add(DatabricksSqlParameter.Long("minPointCount", minPointCount.Value));
        }

        return parameters;
    }
}

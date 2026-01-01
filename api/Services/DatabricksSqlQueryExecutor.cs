using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text.Json;

namespace Trimble.Geospatial.Api.Services;

public sealed class DatabricksSqlQueryExecutor
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly DatabricksSqlClient _client;
    private readonly ILogger<DatabricksSqlQueryExecutor> _logger;

    public DatabricksSqlQueryExecutor(DatabricksSqlClient client, ILogger<DatabricksSqlQueryExecutor> logger)
    {
        _client = client;
        _logger = logger;
    }

    public async Task<T?> QuerySingleAsync<T>(string queryName, string sql, IReadOnlyCollection<DatabricksSqlParameter> parameters, CancellationToken cancellationToken)
    {
        var rows = await ExecuteAsync(queryName, sql, parameters, cancellationToken);
        if (rows.Count == 0)
        {
            return default;
        }

        return DeserializeRow<T>(rows[0], queryName);
    }

    public async Task<IReadOnlyList<T>> QueryListAsync<T>(string queryName, string sql, IReadOnlyCollection<DatabricksSqlParameter> parameters, CancellationToken cancellationToken)
    {
        var rows = await ExecuteAsync(queryName, sql, parameters, cancellationToken);
        var results = new List<T>(rows.Count);

        foreach (var row in rows)
        {
            results.Add(DeserializeRow<T>(row, queryName));
        }

        return results;
    }

    private async Task<IReadOnlyList<string[]>> ExecuteAsync(string queryName, string sql, IReadOnlyCollection<DatabricksSqlParameter> parameters, CancellationToken cancellationToken)
    {
        var sw = Stopwatch.StartNew();
        var rows = await _client.ExecuteRowsAsync(sql, parameters, cancellationToken);
        sw.Stop();

        var siteId = parameters.FirstOrDefault(p => string.Equals(p.Name, "siteId", StringComparison.OrdinalIgnoreCase))?.Value?.ToString();
        _logger.LogInformation(
            "Databricks query executed. QueryName={QueryName} SiteId={SiteId} Rows={RowCount} ElapsedMs={ElapsedMs}",
            queryName,
            siteId,
            rows.Count,
            sw.ElapsedMilliseconds);

        return rows;
    }

    private static T DeserializeRow<T>(string[] row, string queryName)
    {
        if (row.Length == 0 || string.IsNullOrWhiteSpace(row[0]))
        {
            throw new DatabricksSqlException($"Databricks query '{queryName}' returned an empty payload.", HttpStatusCode.BadGateway);
        }

        try
        {
            var value = JsonSerializer.Deserialize<T>(row[0], JsonOptions);
            if (value is null)
            {
                throw new DatabricksSqlException($"Databricks query '{queryName}' returned a null payload.", HttpStatusCode.BadGateway);
            }

            return value;
        }
        catch (JsonException)
        {
            throw new DatabricksSqlException($"Databricks query '{queryName}' returned invalid JSON payload.", HttpStatusCode.BadGateway, isTransient: false);
        }
    }
}

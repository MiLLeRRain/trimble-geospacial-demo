using System.Diagnostics;

namespace Trimble.Geospatial.Api.Services;

public sealed class SqlHealthService
{
    private readonly DatabricksSqlClient _client;

    public SqlHealthService(DatabricksSqlClient client)
    {
        _client = client;
    }

    public async Task<SqlHealthResult> CheckAsync(CancellationToken cancellationToken)
    {
        var sw = Stopwatch.StartNew();
        var value = await _client.ExecuteScalarIntAsync("SELECT 1", cancellationToken);
        sw.Stop();

        return new SqlHealthResult(value, (int)sw.ElapsedMilliseconds);
    }
}

public readonly record struct SqlHealthResult(int Value, int ElapsedMs);

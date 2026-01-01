using System.Collections.Generic;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Azure.Core;
using Microsoft.Extensions.Options;
using Trimble.Geospatial.Api.Models;
using Trimble.Geospatial.Api.Options;

namespace Trimble.Geospatial.Api.Services;

public sealed class DatabricksSqlClient
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly HttpClient _httpClient;
    private readonly DatabricksOptions _options;
    private readonly TokenCredential _credential;
    private readonly ILogger<DatabricksSqlClient> _logger;

    public DatabricksSqlClient(HttpClient httpClient, IOptions<DatabricksOptions> options, TokenCredential credential, ILogger<DatabricksSqlClient> logger)
    {
        _httpClient = httpClient;
        _options = options.Value;
        _credential = credential;
        _logger = logger;
    }

    public async Task<int> ExecuteScalarIntAsync(string statement, CancellationToken cancellationToken)
    {
        var response = await ExecuteStatementAsync(statement, null, cancellationToken);
        var row = ExtractFirstRow(response);

        if (row.Length == 0 || !int.TryParse(row[0], out var value))
        {
            throw new DatabricksSqlException("Failed to parse scalar result.", HttpStatusCode.BadGateway);
        }

        return value;
    }

    public async Task<string[]> ExecuteRowAsync(string statement, CancellationToken cancellationToken)
    {
        var response = await ExecuteStatementAsync(statement, null, cancellationToken);
        return ExtractFirstRow(response);
    }

    public async Task<IReadOnlyList<string[]>> ExecuteRowsAsync(string statement, IReadOnlyCollection<DatabricksSqlParameter>? parameters, CancellationToken cancellationToken)
    {
        var response = await ExecuteStatementAsync(statement, parameters, cancellationToken);
        return ExtractRows(response);
    }

    private async Task<DatabricksStatementResponse> ExecuteStatementAsync(string statement, IReadOnlyCollection<DatabricksSqlParameter>? parameters, CancellationToken cancellationToken)
    {
        var accessToken = await _credential.GetTokenAsync(new TokenRequestContext(new[] { _options.AadScope }), cancellationToken);

        var warehouseId = _options.GetWarehouseId();

        var payload = new Dictionary<string, object?>
        {
            ["warehouse_id"] = warehouseId,
            ["statement"] = statement,
            ["wait_timeout"] = "30s",
            ["on_wait_timeout"] = "FAIL"
        };

        if (parameters is { Count: > 0 })
        {
            payload["parameters"] = parameters;
        }

        using var request = new HttpRequestMessage(HttpMethod.Post, "/api/2.0/sql/statements/")
        {
            Content = new StringContent(JsonSerializer.Serialize(payload, JsonOptions), Encoding.UTF8, "application/json")
        };

        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken.Token);

        _logger.LogDebug("Databricks payload: {Payload}", JsonSerializer.Serialize(payload, JsonOptions));

        using var response = await _httpClient.SendAsync(request, cancellationToken);
        var content = await response.Content.ReadAsStringAsync(cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            _logger.LogWarning(
                "Databricks statement execution failed with status {StatusCode}. WarehouseId={WarehouseId}. Body={Body}",
                response.StatusCode,
                warehouseId,
                content);
            throw new DatabricksSqlException("Databricks SQL execution failed.", response.StatusCode, isTransient: IsTransientStatus(response.StatusCode));
        }

        var statementResponse = JsonSerializer.Deserialize<DatabricksStatementResponse>(content, JsonOptions);
        if (statementResponse is null)
        {
            throw new DatabricksSqlException("Failed to deserialize Databricks response.", HttpStatusCode.BadGateway);
        }

        if (!string.Equals(statementResponse.Status.State, "SUCCEEDED", StringComparison.OrdinalIgnoreCase))
        {
            var error = statementResponse.Status.Error;
            var message = error?.Message ?? "Databricks SQL statement did not succeed.";
            throw new DatabricksSqlException(message, HttpStatusCode.BadGateway, error?.ErrorCode);
        }

        return statementResponse;
    }

    private static IReadOnlyList<string[]> ExtractRows(DatabricksStatementResponse response)
    {
        if (response.Result is null || response.Result.DataArray.ValueKind == JsonValueKind.Undefined)
        {
            throw new DatabricksSqlException("Databricks response did not include result data.", HttpStatusCode.BadGateway);
        }

        var rows = response.Result.DataArray;
        if (rows.ValueKind != JsonValueKind.Array)
        {
            throw new DatabricksSqlException("Databricks response result shape is invalid.", HttpStatusCode.BadGateway);
        }

        var results = new List<string[]>(rows.GetArrayLength());
        foreach (var row in rows.EnumerateArray())
        {
            if (row.ValueKind != JsonValueKind.Array)
            {
                throw new DatabricksSqlException("Databricks response row shape is invalid.", HttpStatusCode.BadGateway);
            }

            var values = new string[row.GetArrayLength()];
            for (var i = 0; i < values.Length; i++)
            {
                var value = row[i];
                values[i] = value.ValueKind == JsonValueKind.String ? value.GetString() ?? string.Empty : value.ToString();
            }

            results.Add(values);
        }

        return results;
    }

    private static string[] ExtractFirstRow(DatabricksStatementResponse response)
    {
        var rows = ExtractRows(response);
        if (rows.Count == 0)
        {
            throw new DatabricksSqlException("Databricks response returned no rows.", HttpStatusCode.BadGateway);
        }

        return rows[0];
    }

    private static bool IsTransientStatus(HttpStatusCode statusCode)
    {
        return statusCode is HttpStatusCode.RequestTimeout
            or HttpStatusCode.TooManyRequests
            or HttpStatusCode.BadGateway
            or HttpStatusCode.ServiceUnavailable
            or HttpStatusCode.GatewayTimeout;
    }
}

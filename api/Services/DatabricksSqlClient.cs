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
        var response = await ExecuteStatementAsync(statement, cancellationToken);
        var row = ExtractFirstRow(response);

        if (row.Length == 0 || !int.TryParse(row[0], out var value))
        {
            throw new DatabricksSqlException("Failed to parse scalar result.", HttpStatusCode.BadGateway);
        }

        return value;
    }

    public async Task<string[]> ExecuteRowAsync(string statement, CancellationToken cancellationToken)
    {
        var response = await ExecuteStatementAsync(statement, cancellationToken);
        return ExtractFirstRow(response);
    }

    private async Task<DatabricksStatementResponse> ExecuteStatementAsync(string statement, CancellationToken cancellationToken)
    {
        var accessToken = await _credential.GetTokenAsync(new TokenRequestContext(new[] { _options.AadScope }), cancellationToken);

        var payload = new
        {
            warehouse_id = _options.GetWarehouseId(),
            statement,
            wait_timeout = "30s",
            on_wait_timeout = "FAIL"
        };

        using var request = new HttpRequestMessage(HttpMethod.Post, "/api/2.0/sql/statements/")
        {
            Content = new StringContent(JsonSerializer.Serialize(payload, JsonOptions), Encoding.UTF8, "application/json")
        };

        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken.Token);

        using var response = await _httpClient.SendAsync(request, cancellationToken);
        var content = await response.Content.ReadAsStringAsync(cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            _logger.LogWarning("Databricks statement execution failed with status {StatusCode}: {Body}", response.StatusCode, content);
            throw new DatabricksSqlException("Databricks SQL execution failed.", response.StatusCode);
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

    private static string[] ExtractFirstRow(DatabricksStatementResponse response)
    {
        if (response.Result is null || response.Result.DataArray.ValueKind == JsonValueKind.Undefined)
        {
            throw new DatabricksSqlException("Databricks response did not include result data.", HttpStatusCode.BadGateway);
        }

        var rows = response.Result.DataArray;
        if (rows.ValueKind != JsonValueKind.Array || rows.GetArrayLength() == 0)
        {
            throw new DatabricksSqlException("Databricks response returned no rows.", HttpStatusCode.BadGateway);
        }

        var firstRow = rows[0];
        if (firstRow.ValueKind != JsonValueKind.Array)
        {
            throw new DatabricksSqlException("Databricks response row shape is invalid.", HttpStatusCode.BadGateway);
        }

        var values = new string[firstRow.GetArrayLength()];
        for (var i = 0; i < values.Length; i++)
        {
            var value = firstRow[i];
            values[i] = value.ValueKind == JsonValueKind.String ? value.GetString() ?? string.Empty : value.ToString();
        }

        return values;
    }
}

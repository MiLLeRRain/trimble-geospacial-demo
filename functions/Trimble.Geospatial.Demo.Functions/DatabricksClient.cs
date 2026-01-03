using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Azure.Core;
using Azure.Identity;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Trimble.Geospatial.Demo.Functions.Options;

namespace Trimble.Geospatial.Demo.Functions;

public sealed class DatabricksClient
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly DatabricksOptions _options;
    private readonly ILogger<DatabricksClient> _logger;
    private readonly DefaultAzureCredential _credential = new();
    private readonly SemaphoreSlim _tokenLock = new(1, 1);
    private AccessToken _cachedToken;

    public DatabricksClient(IHttpClientFactory httpClientFactory, IOptions<DatabricksOptions> options, ILogger<DatabricksClient> logger)
    {
        _httpClientFactory = httpClientFactory;
        _options = options.Value;
        _logger = logger;
    }

    public async Task<long> RunNowAsync(string landingPath, string uploadJobId, string siteId, string ingestRunId, CancellationToken cancellationToken)
    {
        if (!long.TryParse(_options.JobId, out var jobId))
        {
            throw new InvalidOperationException("DATABRICKS_JOB_ID must be a valid integer.");
        }

        var workspaceUrl = _options.WorkspaceUrl?.TrimEnd('/');
        if (string.IsNullOrWhiteSpace(workspaceUrl))
        {
            throw new InvalidOperationException("DATABRICKS_WORKSPACE_URL is not configured.");
        }

        var requestUri = $"{workspaceUrl}/api/2.1/jobs/run-now";
        var payload = new
        {
            job_id = jobId,
            job_parameters = new Dictionary<string, string>
            {
                ["landing_path"] = landingPath,
                ["uploadJobId"] = uploadJobId,
                ["siteId"] = siteId,
                ["ingestRunId"] = ingestRunId
            }
        };

        using var request = new HttpRequestMessage(HttpMethod.Post, requestUri)
        {
            Content = new StringContent(JsonSerializer.Serialize(payload, JsonOptions), Encoding.UTF8, "application/json")
        };

        await ApplyAuthHeaderAsync(request, cancellationToken);

        var client = _httpClientFactory.CreateClient("Databricks");
        using var response = await client.SendAsync(request, cancellationToken);
        var body = await response.Content.ReadAsStringAsync(cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            var trimmed = Trim(body, 1024);
            throw new InvalidOperationException($"Databricks run-now failed: {(int)response.StatusCode} {response.ReasonPhrase}. {trimmed}");
        }

        using var document = JsonDocument.Parse(body);
        var runId = GetLongProperty(document.RootElement, "run_id", "runId");
        if (runId is null)
        {
            throw new InvalidOperationException("Databricks run-now response did not include run_id.");
        }

        _logger.LogInformation("Databricks run-now triggered. jobId={JobId} runId={RunId}", jobId, runId.Value);
        return runId.Value;
    }

    private async Task ApplyAuthHeaderAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        if (!string.IsNullOrWhiteSpace(_options.Token))
        {
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _options.Token);
            return;
        }

        var token = await GetAccessTokenAsync(cancellationToken);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
    }

    private async Task<string> GetAccessTokenAsync(CancellationToken cancellationToken)
    {
        if (_cachedToken.ExpiresOn > DateTimeOffset.UtcNow.AddMinutes(5))
        {
            return _cachedToken.Token;
        }

        await _tokenLock.WaitAsync(cancellationToken);
        try
        {
            if (_cachedToken.ExpiresOn > DateTimeOffset.UtcNow.AddMinutes(5))
            {
                return _cachedToken.Token;
            }

            var context = new TokenRequestContext(new[] { "https://databricks.azure.net/.default" });
            _cachedToken = await _credential.GetTokenAsync(context, cancellationToken);
            return _cachedToken.Token;
        }
        finally
        {
            _tokenLock.Release();
        }
    }

    private static long? GetLongProperty(JsonElement element, params string[] names)
    {
        foreach (var property in element.EnumerateObject())
        {
            foreach (var name in names)
            {
                if (!string.Equals(property.Name, name, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                if (property.Value.ValueKind == JsonValueKind.Number && property.Value.TryGetInt64(out var number))
                {
                    return number;
                }

                if (property.Value.ValueKind == JsonValueKind.String && long.TryParse(property.Value.GetString(), out number))
                {
                    return number;
                }
            }
        }

        return null;
    }

    private static string Trim(string? value, int maxLength)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return string.Empty;
        }

        return value.Length <= maxLength ? value : value[..maxLength];
    }
}

using Microsoft.AspNetCore.Mvc;
using Azure.Core;
using Microsoft.Extensions.Options;
using Trimble.Geospatial.Api.Models;
using Trimble.Geospatial.Api.Options;
using Trimble.Geospatial.Api.Services;
using System.Text;
using System.Text.Json;

namespace Trimble.Geospatial.Api.Controllers;

[ApiController]
[Route("internal")]
public sealed class InternalController : ControllerBase
{
    private readonly SqlHealthService _sqlHealthService;
    private readonly DatabricksSqlClient _databricksSqlClient;
    private readonly TokenCredential _credential;
    private readonly DatabricksOptions _databricksOptions;
    private readonly ILogger<InternalController> _logger;

    public InternalController(
        SqlHealthService sqlHealthService,
        DatabricksSqlClient databricksSqlClient,
        TokenCredential credential,
        IOptions<DatabricksOptions> databricksOptions,
        ILogger<InternalController> logger)
    {
        _sqlHealthService = sqlHealthService;
        _databricksSqlClient = databricksSqlClient;
        _credential = credential;
        _databricksOptions = databricksOptions.Value;
        _logger = logger;
    }

    [HttpGet("health/sql")]
    public async Task<IActionResult> SqlHealth(CancellationToken cancellationToken)
    {
        try
        {
            var result = await _sqlHealthService.CheckAsync(cancellationToken);
            return Ok(new { value = result.Value, elapsedMs = result.ElapsedMs });
        }
        catch (Exception ex)
        {
            var correlationId = HttpContext.GetCorrelationId();
            _logger.LogWarning(ex, "SQL health check failed. CorrelationId={CorrelationId}", correlationId);
            return StatusCode(StatusCodes.Status503ServiceUnavailable, ApiError.From("SqlUnavailable", ex.Message, correlationId));
        }
    }

    [HttpGet("whoami")]
    public async Task<IActionResult> WhoAmI(CancellationToken cancellationToken)
    {
        try
        {
            var row = await _databricksSqlClient.ExecuteRowAsync("SELECT current_user()", cancellationToken);
            return Ok(new
            {
                user = row.ElementAtOrDefault(0) ?? string.Empty
            });
        }
        catch (Exception ex)
        {
            var correlationId = HttpContext.GetCorrelationId();
            _logger.LogWarning(ex, "WhoAmI query failed. CorrelationId={CorrelationId}", correlationId);
            return StatusCode(StatusCodes.Status503ServiceUnavailable, ApiError.From("SqlUnavailable", ex.Message, correlationId));
        }
    }

    // Debug helper: confirms which Entra principal the app is using for Databricks.
    // Returns only selected JWT claims (no token).
    [HttpGet("debug/databricks-token")]
    public async Task<IActionResult> DatabricksTokenInfo(CancellationToken cancellationToken)
    {
        try
        {
            var accessToken = await _credential.GetTokenAsync(
                new TokenRequestContext(new[] { _databricksOptions.AadScope }),
                cancellationToken);

            var claims = JwtPayloadReader.ReadSelectedClaims(accessToken.Token);

            return Ok(new
            {
                scope = _databricksOptions.AadScope,
                expiresOn = accessToken.ExpiresOn,
                claims
            });
        }
        catch (Exception ex)
        {
            var correlationId = HttpContext.GetCorrelationId();
            _logger.LogWarning(ex, "Failed to acquire/parse Databricks token. CorrelationId={CorrelationId}", correlationId);
            return StatusCode(StatusCodes.Status503ServiceUnavailable, ApiError.From("AuthUnavailable", ex.Message, correlationId));
        }
    }
}

internal static class JwtPayloadReader
{
    public static IReadOnlyDictionary<string, object?> ReadSelectedClaims(string jwt)
    {
        // JWT format: header.payload.signature (base64url)
        var parts = jwt.Split('.', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length < 2)
        {
            return new Dictionary<string, object?> { ["error"] = "Invalid JWT format." };
        }

        var payloadJson = Base64UrlDecodeToString(parts[1]);
        using var doc = JsonDocument.Parse(payloadJson);
        var root = doc.RootElement;

        return new Dictionary<string, object?>
        {
            ["tid"] = GetString(root, "tid"),
            ["appid"] = GetString(root, "appid"),
            ["oid"] = GetString(root, "oid"),
            ["azp"] = GetString(root, "azp"),
            ["aud"] = GetString(root, "aud"),
            ["iss"] = GetString(root, "iss"),
            ["xms_mirid"] = GetString(root, "xms_mirid"),
        };
    }

    private static string? GetString(JsonElement root, string name)
    {
        if (!root.TryGetProperty(name, out var value))
        {
            return null;
        }

        return value.ValueKind switch
        {
            JsonValueKind.String => value.GetString(),
            JsonValueKind.Number => value.ToString(),
            JsonValueKind.True => "true",
            JsonValueKind.False => "false",
            _ => value.ToString()
        };
    }

    private static string Base64UrlDecodeToString(string base64Url)
    {
        var padded = base64Url.Replace('-', '+').Replace('_', '/');
        switch (padded.Length % 4)
        {
            case 2:
                padded += "==";
                break;
            case 3:
                padded += "=";
                break;
        }

        var bytes = Convert.FromBase64String(padded);
        return Encoding.UTF8.GetString(bytes);
    }
}

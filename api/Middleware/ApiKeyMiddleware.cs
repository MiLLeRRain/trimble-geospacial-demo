using Microsoft.Extensions.Options;
using Trimble.Geospatial.Api.Models;
using Trimble.Geospatial.Api.Options;

namespace Trimble.Geospatial.Api.Middleware;

public sealed class ApiKeyMiddleware
{
    private readonly RequestDelegate _next;
    private readonly ILogger<ApiKeyMiddleware> _logger;
    private readonly InternalApiOptions _options;

    public ApiKeyMiddleware(RequestDelegate next, ILogger<ApiKeyMiddleware> logger, IOptions<InternalApiOptions> options)
    {
        _next = next;
        _logger = logger;
        _options = options.Value;
    }

    public async Task InvokeAsync(HttpContext context)
    {
        if (!context.Request.Path.StartsWithSegments("/internal", StringComparison.OrdinalIgnoreCase))
        {
            await _next(context);
            return;
        }

        var correlationId = context.GetCorrelationId();

        if (string.IsNullOrWhiteSpace(_options.ApiKey))
        {
            _logger.LogError("INTERNAL_API_KEY is not configured. CorrelationId={CorrelationId}", correlationId);
            context.Response.StatusCode = StatusCodes.Status500InternalServerError;
            context.Response.ContentType = "application/json";
            await context.Response.WriteAsJsonAsync(ApiError.From("ServerMisconfigured", "INTERNAL_API_KEY is not configured.", correlationId));
            return;
        }

        if (!context.Request.Headers.TryGetValue("x-api-key", out var provided) ||
            !string.Equals(provided.ToString(), _options.ApiKey, StringComparison.Ordinal))
        {
            context.Response.StatusCode = StatusCodes.Status401Unauthorized;
            context.Response.ContentType = "application/json";
            await context.Response.WriteAsJsonAsync(ApiError.From("Unauthorized", "Missing or invalid API key.", correlationId));
            return;
        }

        await _next(context);
    }
}

using Microsoft.Extensions.Options;
using Trimble.Geospatial.Api.Models;
using Trimble.Geospatial.Api.Options;

namespace Trimble.Geospatial.Api.Middleware;

public sealed class ApiKeyMiddleware
{
    private readonly RequestDelegate _next;
    private readonly ILogger<ApiKeyMiddleware> _logger;
    private readonly InternalApiOptions _options;
    private readonly PublicApiOptions _publicOptions;

    public ApiKeyMiddleware(
        RequestDelegate next,
        ILogger<ApiKeyMiddleware> logger,
        IOptions<InternalApiOptions> options,
        IOptions<PublicApiOptions> publicOptions)
    {
        _next = next;
        _logger = logger;
        _options = options.Value;
        _publicOptions = publicOptions.Value;
    }

    public async Task InvokeAsync(HttpContext context)
    {
        if (context.Request.Path.Equals("/health", StringComparison.OrdinalIgnoreCase))
        {
            await _next(context);
            return;
        }

        var isInternal = context.Request.Path.StartsWithSegments("/internal", StringComparison.OrdinalIgnoreCase);
        var isPublicApi = context.Request.Path.StartsWithSegments("/api/v1", StringComparison.OrdinalIgnoreCase);

        if (!isInternal && !isPublicApi)
        {
            await _next(context);
            return;
        }

        var correlationId = context.GetCorrelationId();
        var expectedKey = isInternal ? _options.ApiKey : (_publicOptions.ApiKey ?? _options.ApiKey);

        if (string.IsNullOrWhiteSpace(expectedKey))
        {
            _logger.LogError("API key is not configured. CorrelationId={CorrelationId}", correlationId);
            context.Response.StatusCode = StatusCodes.Status500InternalServerError;
            context.Response.ContentType = "application/json";
            await context.Response.WriteAsJsonAsync(ApiError.From("ServerMisconfigured", "API key is not configured.", correlationId));
            return;
        }

        if (!context.Request.Headers.TryGetValue("x-api-key", out var provided) ||
            !string.Equals(provided.ToString(), expectedKey, StringComparison.Ordinal))
        {
            context.Response.StatusCode = StatusCodes.Status401Unauthorized;
            context.Response.ContentType = "application/json";
            await context.Response.WriteAsJsonAsync(ApiError.From("Unauthorized", "Missing or invalid API key.", correlationId));
            return;
        }

        await _next(context);
    }
}

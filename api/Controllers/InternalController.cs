using Microsoft.AspNetCore.Mvc;
using Trimble.Geospatial.Api.Models;
using Trimble.Geospatial.Api.Services;

namespace Trimble.Geospatial.Api.Controllers;

[ApiController]
[Route("internal")]
public sealed class InternalController : ControllerBase
{
    private readonly SqlHealthService _sqlHealthService;
    private readonly DatabricksSqlClient _databricksSqlClient;
    private readonly ILogger<InternalController> _logger;

    public InternalController(SqlHealthService sqlHealthService, DatabricksSqlClient databricksSqlClient, ILogger<InternalController> logger)
    {
        _sqlHealthService = sqlHealthService;
        _databricksSqlClient = databricksSqlClient;
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
}

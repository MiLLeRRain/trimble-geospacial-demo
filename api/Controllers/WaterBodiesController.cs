using Microsoft.AspNetCore.Mvc;
using Trimble.Geospatial.Api.Models;
using Trimble.Geospatial.Api.Repositories;
using Trimble.Geospatial.Api.Services;

namespace Trimble.Geospatial.Api.Controllers;

[ApiController]
[Route("api/v1/sites/{siteId}/water-bodies")]
public sealed class WaterBodiesController : ControllerBase
{
    private const int DefaultLimit = 100;
    private const int MaxLimit = 1000;

    private readonly WaterBodyRepository _repository;
    private readonly ILogger<WaterBodiesController> _logger;

    public WaterBodiesController(WaterBodyRepository repository, ILogger<WaterBodiesController> logger)
    {
        _repository = repository;
        _logger = logger;
    }

    [HttpGet]
    public async Task<IActionResult> GetList(
        string siteId,
        [FromQuery] double? minAreaM2 = null,
        [FromQuery] int? limit = null,
        [FromQuery] string? orderBy = null,
        CancellationToken cancellationToken = default)
    {
        if (!WaterBodyOrderByParser.TryParse(orderBy, out var parsedOrderBy))
        {
            return BadRequest(ApiError.From("InvalidOrderBy", "orderBy must be one of: areaDesc.", HttpContext.GetCorrelationId()));
        }

        var resolvedLimit = limit ?? DefaultLimit;
        if (resolvedLimit <= 0 || resolvedLimit > MaxLimit)
        {
            return BadRequest(ApiError.From("InvalidLimit", $"limit must be between 1 and {MaxLimit}.", HttpContext.GetCorrelationId()));
        }

        if (minAreaM2 is < 0)
        {
            return BadRequest(ApiError.From("InvalidMinAreaM2", "minAreaM2 must be 0 or greater.", HttpContext.GetCorrelationId()));
        }

        try
        {
            var items = await _repository.GetListAsync(siteId, minAreaM2, parsedOrderBy, resolvedLimit, cancellationToken);
            return Ok(items);
        }
        catch (DatabricksSqlException ex)
        {
            return MapSqlError(ex, "GetWaterBodies", siteId);
        }
    }

    [HttpGet("{waterBodyId}")]
    public async Task<IActionResult> GetById(string siteId, string waterBodyId, CancellationToken cancellationToken)
    {
        if (!long.TryParse(waterBodyId, out var parsedId))
        {
            return BadRequest(ApiError.From("InvalidWaterBodyId", "waterBodyId must be a valid integer.", HttpContext.GetCorrelationId()));
        }

        try
        {
            var item = await _repository.GetByIdAsync(siteId, parsedId, cancellationToken);
            if (item is null)
            {
                return NotFound(ApiError.From("WaterBodyNotFound", $"No waterBodyId '{waterBodyId}' found for siteId '{siteId}'.", HttpContext.GetCorrelationId()));
            }

            return Ok(item);
        }
        catch (DatabricksSqlException ex)
        {
            return MapSqlError(ex, "GetWaterBodyById", siteId);
        }
    }

    private ObjectResult MapSqlError(DatabricksSqlException ex, string queryName, string siteId)
    {
        var correlationId = HttpContext.GetCorrelationId();
        _logger.LogWarning(
            ex,
            "Databricks query failed. QueryName={QueryName} SiteId={SiteId} CorrelationId={CorrelationId}",
            queryName,
            siteId,
            correlationId);

        var statusCode = ex.IsTransient ? StatusCodes.Status503ServiceUnavailable : StatusCodes.Status502BadGateway;
        var errorCode = ex.IsTransient ? "SqlUnavailable" : "SqlError";
        var message = ex.IsTransient ? "Databricks SQL is temporarily unavailable." : "Databricks SQL query failed.";

        return StatusCode(statusCode, ApiError.From(errorCode, message, correlationId));
    }
}

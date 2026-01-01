using Microsoft.AspNetCore.Mvc;
using Trimble.Geospatial.Api.Models;
using Trimble.Geospatial.Api.Repositories;
using Trimble.Geospatial.Api.Services;

namespace Trimble.Geospatial.Api.Controllers;

[ApiController]
[Route("api/v1/sites/{siteId}/tiles")]
public sealed class TilesController : ControllerBase
{
    private const int DefaultLimit = 100;
    private const int MaxLimit = 2000;

    private readonly TileStatsRepository _repository;
    private readonly ILogger<TilesController> _logger;

    public TilesController(TileStatsRepository repository, ILogger<TilesController> logger)
    {
        _repository = repository;
        _logger = logger;
    }

    [HttpGet]
    public async Task<IActionResult> GetTiles(
        string siteId,
        [FromQuery] bool skipMostlyWater = false,
        [FromQuery] long? minPointCount = null,
        [FromQuery] int? limit = null,
        [FromQuery] int? offset = null,
        [FromQuery] string? orderBy = null,
        CancellationToken cancellationToken = default)
    {
        if (!TileOrderByParser.TryParse(orderBy, out var parsedOrderBy))
        {
            return BadRequest(ApiError.From("InvalidOrderBy", "orderBy must be one of: tileId, pointCountDesc, waterRatioDesc, heightP99Desc, reliefDesc, computedAtDesc.", HttpContext.GetCorrelationId()));
        }

        var resolvedLimit = limit ?? DefaultLimit;
        if (resolvedLimit <= 0 || resolvedLimit > MaxLimit)
        {
            return BadRequest(ApiError.From("InvalidLimit", $"limit must be between 1 and {MaxLimit}.", HttpContext.GetCorrelationId()));
        }

        var resolvedOffset = offset ?? 0;
        if (resolvedOffset < 0)
        {
            return BadRequest(ApiError.From("InvalidOffset", "offset must be 0 or greater.", HttpContext.GetCorrelationId()));
        }

        if (minPointCount is < 0)
        {
            return BadRequest(ApiError.From("InvalidMinPointCount", "minPointCount must be 0 or greater.", HttpContext.GetCorrelationId()));
        }

        try
        {
            var tiles = await _repository.GetTilesAsync(
                siteId,
                skipMostlyWater,
                minPointCount,
                parsedOrderBy,
                resolvedLimit,
                resolvedOffset,
                cancellationToken);

            return Ok(tiles);
        }
        catch (DatabricksSqlException ex)
        {
            return MapSqlError(ex, siteId);
        }
    }

    private ObjectResult MapSqlError(DatabricksSqlException ex, string siteId)
    {
        var correlationId = HttpContext.GetCorrelationId();
        _logger.LogWarning(
            ex,
            "Databricks query failed. QueryName=GetTileStats SiteId={SiteId} CorrelationId={CorrelationId}",
            siteId,
            correlationId);

        var statusCode = ex.IsTransient ? StatusCodes.Status503ServiceUnavailable : StatusCodes.Status502BadGateway;
        var errorCode = ex.IsTransient ? "SqlUnavailable" : "SqlError";
        var message = ex.IsTransient ? "Databricks SQL is temporarily unavailable." : "Databricks SQL query failed.";

        return StatusCode(statusCode, ApiError.From(errorCode, message, correlationId));
    }
}

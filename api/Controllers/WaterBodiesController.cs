using Microsoft.AspNetCore.Mvc;
using Trimble.Geospatial.Api.Models;
using Trimble.Geospatial.Api.Repositories;
using Trimble.Geospatial.Api.Services;

namespace Trimble.Geospatial.Api.Controllers;

/// <summary>
/// Provides water body features for a site.
/// </summary>
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

    /// <summary>
    /// List water bodies for a site.
    /// </summary>
    /// <remarks>
    /// Returns detected water body features and their bounding boxes for the site.
    /// </remarks>
    /// <param name="siteId">Site identifier for the dataset.</param>
    /// <param name="minAreaM2">Minimum water body area in square meters.</param>
    /// <param name="limit">Maximum number of items to return.</param>
    /// <param name="orderBy">Sort order for the water bodies.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <response code="200">Water bodies for the requested site.</response>
    /// <response code="401">Missing or invalid API key.</response>
    /// <response code="400">Invalid query parameter.</response>
    /// <response code="503">Databricks SQL is temporarily unavailable.</response>
    /// <response code="502">Databricks SQL query failed.</response>
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

    /// <summary>
    /// Get a water body by ID.
    /// </summary>
    /// <remarks>
    /// Returns a single water body feature for the site.
    /// </remarks>
    /// <param name="siteId">Site identifier for the dataset.</param>
    /// <param name="waterBodyId">Water body identifier.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <response code="200">Water body details.</response>
    /// <response code="401">Missing or invalid API key.</response>
    /// <response code="400">The waterBodyId path value is invalid.</response>
    /// <response code="404">The water body was not found for the site.</response>
    /// <response code="503">Databricks SQL is temporarily unavailable.</response>
    /// <response code="502">Databricks SQL query failed.</response>
    [HttpGet("{waterBodyId}")]
    public async Task<IActionResult> GetById(
        string siteId,
        string waterBodyId,
        CancellationToken cancellationToken)
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

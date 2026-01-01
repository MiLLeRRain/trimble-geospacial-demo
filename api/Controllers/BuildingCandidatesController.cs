using Microsoft.AspNetCore.Mvc;
using Trimble.Geospatial.Api.Models;
using Trimble.Geospatial.Api.Repositories;
using Trimble.Geospatial.Api.Services;

namespace Trimble.Geospatial.Api.Controllers;

/// <summary>
/// Provides building candidate features for a site.
/// </summary>
[ApiController]
[Route("api/v1/sites/{siteId}/building-candidates")]
public sealed class BuildingCandidatesController : ControllerBase
{
    private const int DefaultLimit = 100;
    private const int MaxLimit = 1000;

    private readonly BuildingCandidateRepository _repository;
    private readonly ILogger<BuildingCandidatesController> _logger;

    public BuildingCandidatesController(BuildingCandidateRepository repository, ILogger<BuildingCandidatesController> logger)
    {
        _repository = repository;
        _logger = logger;
    }

    [HttpGet]
    /// <summary>
    /// List building candidates for a site.
    /// </summary>
    /// <remarks>
    /// Returns detected building candidate features with height metrics for the site.
    /// </remarks>
    /// <param name="siteId">Site identifier for the dataset.</param>
    /// <param name="tileId">Filter by a specific tile identifier.</param>
    /// <param name="minHeight">Minimum height above ground in meters.</param>
    /// <param name="limit">Maximum number of items to return.</param>
    /// <param name="orderBy">Sort order for the building candidates.</param>
    /// <response code="200">Building candidates for the requested site.</response>
    /// <response code="401">Missing or invalid API key.</response>
    /// <response code="400">Invalid query parameter.</response>
    /// <response code="503">Databricks SQL is temporarily unavailable.</response>
    /// <response code="502">Databricks SQL query failed.</response>
    public async Task<IActionResult> GetList(
        string siteId,
        [FromQuery] string? tileId = null,
        [FromQuery] double? minHeight = null,
        [FromQuery] int? limit = null,
        [FromQuery] string? orderBy = null,
        CancellationToken cancellationToken = default)
    {
        if (!BuildingCandidateOrderByParser.TryParse(orderBy, out var parsedOrderBy))
        {
            return BadRequest(ApiError.From("InvalidOrderBy", "orderBy must be one of: heightDesc, heightRangeDesc.", HttpContext.GetCorrelationId()));
        }

        var resolvedLimit = limit ?? DefaultLimit;
        if (resolvedLimit <= 0 || resolvedLimit > MaxLimit)
        {
            return BadRequest(ApiError.From("InvalidLimit", $"limit must be between 1 and {MaxLimit}.", HttpContext.GetCorrelationId()));
        }

        if (minHeight is < 0)
        {
            return BadRequest(ApiError.From("InvalidMinHeight", "minHeight must be 0 or greater.", HttpContext.GetCorrelationId()));
        }

        try
        {
            var items = await _repository.GetListAsync(siteId, tileId, minHeight, parsedOrderBy, resolvedLimit, cancellationToken);
            return Ok(items);
        }
        catch (DatabricksSqlException ex)
        {
            return MapSqlError(ex, "GetBuildingCandidates", siteId);
        }
    }

    [HttpGet("/api/v1/sites/{siteId}/tiles/{tileId}/building-candidates/{candidateId}")]
    /// <summary>
    /// Get a building candidate by ID.
    /// </summary>
    /// <remarks>
    /// Returns a single building candidate feature for the site and tile.
    /// </remarks>
    /// <param name="siteId">Site identifier for the dataset.</param>
    /// <param name="tileId">Tile identifier within the site.</param>
    /// <param name="candidateId">Building candidate identifier.</param>
    /// <response code="200">Building candidate details.</response>
    /// <response code="401">Missing or invalid API key.</response>
    /// <response code="404">The building candidate was not found for the site and tile.</response>
    /// <response code="503">Databricks SQL is temporarily unavailable.</response>
    /// <response code="502">Databricks SQL query failed.</response>
    public async Task<IActionResult> GetById(
        string siteId,
        string tileId,
        string candidateId,
        CancellationToken cancellationToken)
    {
        try
        {
            var item = await _repository.GetByIdAsync(siteId, tileId, candidateId, cancellationToken);
            if (item is null)
            {
                return NotFound(ApiError.From("BuildingCandidateNotFound", $"No candidate '{candidateId}' found for siteId '{siteId}' and tileId '{tileId}'.", HttpContext.GetCorrelationId()));
            }

            return Ok(item);
        }
        catch (DatabricksSqlException ex)
        {
            return MapSqlError(ex, "GetBuildingCandidateById", siteId);
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

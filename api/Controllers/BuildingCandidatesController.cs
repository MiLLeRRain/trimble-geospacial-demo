using Microsoft.AspNetCore.Mvc;
using Trimble.Geospatial.Api.Models;
using Trimble.Geospatial.Api.Repositories;
using Trimble.Geospatial.Api.Services;

namespace Trimble.Geospatial.Api.Controllers;

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

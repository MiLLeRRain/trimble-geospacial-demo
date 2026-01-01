using Microsoft.AspNetCore.Mvc;
using Trimble.Geospatial.Api.Models;
using Trimble.Geospatial.Api.Repositories;
using Trimble.Geospatial.Api.Services;

namespace Trimble.Geospatial.Api.Controllers;

/// <summary>
/// Provides pipeline run status and metadata for a site.
/// </summary>
[ApiController]
[Route("api/v1/sites/{siteId}/runs")]
public sealed class RunsController : ControllerBase
{
    private readonly PipelineRunRepository _repository;
    private readonly ILogger<RunsController> _logger;

    public RunsController(PipelineRunRepository repository, ILogger<RunsController> logger)
    {
        _repository = repository;
        _logger = logger;
    }

    /// <summary>
    /// Get the latest pipeline run for a site.
    /// </summary>
    /// <remarks>
    /// Returns status and timing details for the most recent processing run for the requested site.
    /// </remarks>
    /// <param name="siteId">Site identifier for the dataset.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <response code="200">Latest pipeline run status.</response>
    /// <response code="401">Missing or invalid API key.</response>
    /// <response code="404">No runs exist for the site.</response>
    /// <response code="503">Databricks SQL is temporarily unavailable.</response>
    /// <response code="502">Databricks SQL query failed.</response>
    [HttpGet("latest")]
    public async Task<IActionResult> GetLatest(
        string siteId,
        CancellationToken cancellationToken)
    {
        const string queryName = "GetLatestPipelineRun";

        try
        {
            var run = await _repository.GetLatestAsync(siteId, cancellationToken);
            if (run is null)
            {
                return NotFound(ApiError.From("RunNotFound", $"No runs found for siteId '{siteId}'.", HttpContext.GetCorrelationId()));
            }

            return Ok(run);
        }
        catch (DatabricksSqlException ex)
        {
            return MapSqlError(ex, queryName, siteId);
        }
    }

    /// <summary>
    /// Get a pipeline run by ID.
    /// </summary>
    /// <remarks>
    /// Returns status and timing details for a specific processing run.
    /// </remarks>
    /// <param name="siteId">Site identifier for the dataset.</param>
    /// <param name="runId">Pipeline run identifier.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <response code="200">Pipeline run status.</response>
    /// <response code="401">Missing or invalid API key.</response>
    /// <response code="404">The run ID does not exist for the site.</response>
    /// <response code="503">Databricks SQL is temporarily unavailable.</response>
    /// <response code="502">Databricks SQL query failed.</response>
    [HttpGet("{runId}")]
    public async Task<IActionResult> GetById(
        string siteId,
        string runId,
        CancellationToken cancellationToken)
    {
        const string queryName = "GetPipelineRunById";

        try
        {
            var run = await _repository.GetByIdAsync(siteId, runId, cancellationToken);
            if (run is null)
            {
                return NotFound(ApiError.From("RunNotFound", $"No run '{runId}' found for siteId '{siteId}'.", HttpContext.GetCorrelationId()));
            }

            return Ok(run);
        }
        catch (DatabricksSqlException ex)
        {
            return MapSqlError(ex, queryName, siteId);
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

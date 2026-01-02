using Microsoft.AspNetCore.Mvc;
using Trimble.Geospatial.Api.Models;
using Trimble.Geospatial.Api.Repositories;

namespace Trimble.Geospatial.Api.Controllers;

[ApiController]
[Route("api/v1/jobs")]
public sealed class JobsController : ControllerBase
{
    private readonly JobDbTestRepository _repo;
    private readonly ILogger<JobsController> _logger;

    public JobsController(JobDbTestRepository repo, ILogger<JobsController> logger)
    {
        _repo = repo;
        _logger = logger;
    }

    // Minimal DB connectivity smoke test.
    [HttpPost("_test")]
    public async Task<IActionResult> Test(CancellationToken cancellationToken)
    {
        try
        {
            var result = await _repo.InsertAndSelectAsync(cancellationToken);
            return Ok(result);
        }
        catch (Exception ex)
        {
            var correlationId = HttpContext.GetCorrelationId();
            _logger.LogWarning(ex, "Job DB test failed. CorrelationId={CorrelationId}", correlationId);
            return StatusCode(StatusCodes.Status503ServiceUnavailable,
                ApiError.From("JobDbUnavailable", ex.Message, correlationId));
        }
    }
}

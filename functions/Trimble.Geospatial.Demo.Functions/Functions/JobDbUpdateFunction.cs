using System.Net;
using System.Text.Json;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using Trimble.Geospatial.Demo.Functions.Repositories;

namespace Trimble.Geospatial.Demo.Functions.Functions;

public sealed class JobDbUpdateFunction
{
    private readonly JobDbRepository _repo;
    private readonly ILogger<JobDbUpdateFunction> _logger;

    public JobDbUpdateFunction(JobDbRepository repo, ILogger<JobDbUpdateFunction> logger)
    {
        _repo = repo;
        _logger = logger;
    }

    // Minimal HTTP-triggered function for Step 2.
    // Usage:
    //   POST /api/jobdb/update?jobId=<id>&status=FUNCTION_TESTED
    // If jobId is omitted, it updates the latest job by CreatedAtUtc.
    [Function("jobdb-update")]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Function, "post", Route = "jobdb/update")] HttpRequestData req,
        CancellationToken cancellationToken)
    {
        try
        {
            var query = System.Web.HttpUtility.ParseQueryString(req.Url.Query);
            var jobId = query.Get("jobId");
            var status = query.Get("status") ?? "FUNCTION_TESTED";

            if (string.IsNullOrWhiteSpace(jobId))
            {
                jobId = await _repo.GetLatestJobIdAsync(cancellationToken);
            }

            if (string.IsNullOrWhiteSpace(jobId))
            {
                var notFound = req.CreateResponse(HttpStatusCode.NotFound);
                await notFound.WriteStringAsync("No jobId provided and no Jobs rows exist.", cancellationToken);
                return notFound;
            }

            var (currentDb, loginName) = await _repo.GetIdentityAsync(cancellationToken);
            var affected = await _repo.UpdateJobStatusAsync(jobId, status, cancellationToken);

            var res = req.CreateResponse(affected > 0 ? HttpStatusCode.OK : HttpStatusCode.NotFound);
            res.Headers.Add("Content-Type", "application/json");

            await res.WriteStringAsync(JsonSerializer.Serialize(new
            {
                currentDb,
                loginName,
                jobId,
                status,
                rowsAffected = affected
            }), cancellationToken);

            return res;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Job DB update failed");
            var res = req.CreateResponse(HttpStatusCode.ServiceUnavailable);
            await res.WriteStringAsync(ex.Message, cancellationToken);
            return res;
        }
    }
}

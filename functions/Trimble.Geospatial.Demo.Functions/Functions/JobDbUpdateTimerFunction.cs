using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Trimble.Geospatial.Demo.Functions.Repositories;

namespace Trimble.Geospatial.Demo.Functions.Functions;

public sealed class JobDbUpdateTimerFunction
{
    private readonly JobDbRepository _repo;
    private readonly ILogger<JobDbUpdateTimerFunction> _logger;

    public JobDbUpdateTimerFunction(JobDbRepository repo, ILogger<JobDbUpdateTimerFunction> logger)
    {
        _repo = repo;
        _logger = logger;
    }

    // Optional: timer trigger to prove the function is "alive" without manual HTTP calls.
    // Every 5 minutes.
    [Function("jobdb-update-timer")]
    public async Task Run([TimerTrigger("0 */5 * * * *")] TimerInfo timer, CancellationToken cancellationToken)
    {
        try
        {
            var jobId = await _repo.GetLatestJobIdAsync(cancellationToken);
            if (string.IsNullOrWhiteSpace(jobId))
            {
                _logger.LogInformation("Timer fired but no Jobs rows exist.");
                return;
            }

            var (currentDb, loginName) = await _repo.GetIdentityAsync(cancellationToken);
            var affected = await _repo.UpdateJobStatusAsync(jobId, "FUNCTION_TIMER_TICK", cancellationToken);

            _logger.LogInformation("Updated job status. currentDb={CurrentDb} loginName={LoginName} jobId={JobId} affected={Affected}", currentDb, loginName, jobId, affected);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Timer job DB update failed");
        }
    }
}

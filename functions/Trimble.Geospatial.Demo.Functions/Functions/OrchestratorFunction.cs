using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Trimble.Geospatial.Demo.Functions.Repositories;

namespace Trimble.Geospatial.Demo.Functions.Functions;

public sealed class OrchestratorFunction
{
    private readonly JobRepository _repository;
    private readonly DatabricksClient _databricksClient;
    private readonly ILogger<OrchestratorFunction> _logger;

    public OrchestratorFunction(JobRepository repository, DatabricksClient databricksClient, ILogger<OrchestratorFunction> logger)
    {
        _repository = repository;
        _databricksClient = databricksClient;
        _logger = logger;
    }

    [Function("job-init-orchestrator")]
    public async Task Run(
        [ServiceBusTrigger("%SERVICEBUS__QUEUE_NAME%", Connection = "SERVICEBUS__CONNECTION")] string message,
        ServiceBusReceivedMessage rawMessage,
        CancellationToken cancellationToken)
    {
        var payload = ParsePayload(message);

        if (string.IsNullOrWhiteSpace(payload.JobId) ||
            string.IsNullOrWhiteSpace(payload.LandingPath) ||
            string.IsNullOrWhiteSpace(payload.SiteId) ||
            string.IsNullOrWhiteSpace(payload.IngestRunId))
        {
            throw new InvalidOperationException("job_init message missing required fields.");
        }

        var updated = await _repository.TryMarkDbxTriggeredAsync(payload.JobId, payload.LandingPath, payload.IngestRunId, cancellationToken);
        if (updated == 0)
        {
            var currentStatus = await _repository.GetJobStatusAsync(payload.JobId, cancellationToken);
            if (string.IsNullOrWhiteSpace(currentStatus))
            {
                _logger.LogWarning("Job not found. jobId={JobId} messageId={MessageId}", payload.JobId, rawMessage.MessageId);
                return;
            }

            _logger.LogInformation("Job already processed. jobId={JobId} status={Status} messageId={MessageId}", payload.JobId, currentStatus, rawMessage.MessageId);
            return;
        }

        try
        {
            var runId = await _databricksClient.RunNowAsync(payload.LandingPath, payload.JobId, payload.SiteId, payload.IngestRunId, cancellationToken);
            await _repository.SetProcessingAsync(payload.JobId, runId, cancellationToken);
            _logger.LogInformation("Job marked processing. jobId={JobId} runId={RunId}", payload.JobId, runId);
        }
        catch (Exception ex)
        {
            var errorMessage = Trim(ex.Message, 2048);
            await _repository.SetFailedAsync(payload.JobId, "DBX_TRIGGER_FAILED", errorMessage, cancellationToken);
            _logger.LogError(ex, "Databricks trigger failed. jobId={JobId}", payload.JobId);
            throw;
        }
    }

    private static JobInitPayload ParsePayload(string message)
    {
        using var document = JsonDocument.Parse(message);
        if (document.RootElement.ValueKind != JsonValueKind.Object)
        {
            throw new InvalidOperationException("job_init payload must be a JSON object.");
        }

        var root = document.RootElement;
        return new JobInitPayload(
            GetString(root, "jobId", "job_id"),
            GetString(root, "landing_path", "landingPath"),
            GetString(root, "siteId", "site_id"),
            GetString(root, "ingestRunId", "targetIngestRunId", "ingest_run_id"));
    }

    private static string? GetString(JsonElement element, params string[] names)
    {
        foreach (var property in element.EnumerateObject())
        {
            foreach (var name in names)
            {
                if (string.Equals(property.Name, name, StringComparison.OrdinalIgnoreCase))
                {
                    return property.Value.GetString();
                }
            }
        }

        return null;
    }

    private static string? Trim(string? value, int maxLength)
        => string.IsNullOrWhiteSpace(value) ? null : (value.Length <= maxLength ? value : value[..maxLength]);

    private sealed record JobInitPayload(string? JobId, string? LandingPath, string? SiteId, string? IngestRunId);
}

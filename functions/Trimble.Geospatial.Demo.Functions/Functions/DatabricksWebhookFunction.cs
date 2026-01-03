using System.Net;
using System.Text.Json;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using Trimble.Geospatial.Demo.Functions.Repositories;

namespace Trimble.Geospatial.Demo.Functions.Functions;

public sealed class DatabricksWebhookFunction
{
    private static readonly HashSet<string> RunIdNames = new(StringComparer.OrdinalIgnoreCase) { "run_id", "runId" };
    private static readonly HashSet<string> JobIdNames = new(StringComparer.OrdinalIgnoreCase) { "job_id", "jobId" };
    private static readonly HashSet<string> StatusNames = new(StringComparer.OrdinalIgnoreCase) { "result_state", "resultState", "status" };
    private static readonly HashSet<string> ErrorNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "error_message", "errorMessage", "message", "error", "state_message", "stateMessage"
    };

    private readonly JobRepository _repository;
    private readonly WebhookSignatureValidator _signatureValidator;
    private readonly ILogger<DatabricksWebhookFunction> _logger;

    public DatabricksWebhookFunction(JobRepository repository, WebhookSignatureValidator signatureValidator, ILogger<DatabricksWebhookFunction> logger)
    {
        _repository = repository;
        _signatureValidator = signatureValidator;
        _logger = logger;
    }

    [Function("dbx-webhook")]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "dbx/webhook")] HttpRequestData req,
        CancellationToken cancellationToken)
    {
        var bodyBytes = await ReadBodyBytesAsync(req, cancellationToken);
        var signature = req.Headers.TryGetValues("X-Dbx-Signature", out var values) ? values.FirstOrDefault() : null;

        if (!_signatureValidator.IsValidSignature(signature, bodyBytes))
        {
            _logger.LogWarning("Webhook signature invalid.");
            return req.CreateResponse(HttpStatusCode.Unauthorized);
        }

        WebhookPayload payload;
        try
        {
            payload = ParsePayload(bodyBytes);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Webhook payload invalid.");
            var bad = req.CreateResponse(HttpStatusCode.BadRequest);
            await bad.WriteStringAsync("Invalid webhook payload.", cancellationToken);
            return bad;
        }

        if (!payload.RunId.HasValue)
        {
            var bad = req.CreateResponse(HttpStatusCode.BadRequest);
            await bad.WriteStringAsync("Missing run_id.", cancellationToken);
            return bad;
        }

        if (!payload.IsSuccess.HasValue)
        {
            var bad = req.CreateResponse(HttpStatusCode.BadRequest);
            await bad.WriteStringAsync("Missing result state.", cancellationToken);
            return bad;
        }

        var status = payload.IsSuccess.Value ? "SUCCEEDED" : "FAILED";
        var errorCode = payload.IsSuccess.Value ? null : "DBX_RUN_FAILED";
        var errorMessage = payload.IsSuccess.Value ? null : payload.ErrorMessage;

        var updated = await _repository.UpdateFromWebhookByRunIdAsync(payload.RunId.Value, status, errorCode, errorMessage, cancellationToken);
        if (updated == 0 && payload.JobId.HasValue)
        {
            updated = await _repository.UpdateFromWebhookByJobIdAsync(payload.JobId.Value.ToString(), status, errorCode, errorMessage, cancellationToken);
        }

        if (updated == 0)
        {
            _logger.LogWarning("Webhook received but no matching job found. runId={RunId} jobId={JobId}", payload.RunId, payload.JobId);
            return req.CreateResponse(HttpStatusCode.Accepted);
        }

        _logger.LogInformation("Webhook processed. runId={RunId} jobId={JobId} status={Status}", payload.RunId, payload.JobId, status);
        return req.CreateResponse(HttpStatusCode.OK);
    }

    private static async Task<byte[]> ReadBodyBytesAsync(HttpRequestData req, CancellationToken cancellationToken)
    {
        using var memory = new MemoryStream();
        await req.Body.CopyToAsync(memory, cancellationToken);
        return memory.ToArray();
    }

    private static WebhookPayload ParsePayload(byte[] bodyBytes)
    {
        using var document = JsonDocument.Parse(bodyBytes);
        var root = document.RootElement;

        var runId = TryFindLong(root, RunIdNames);
        var jobId = TryFindLong(root, JobIdNames);
        var status = TryFindString(root, StatusNames);
        var error = TryFindString(root, ErrorNames);

        return new WebhookPayload(runId, jobId, DetermineSuccess(status), Trim(error, 2048));
    }

    private static long? TryFindLong(JsonElement element, HashSet<string> names)
    {
        if (TryFindValue(element, names, out var value))
        {
            if (value.ValueKind == JsonValueKind.Number && value.TryGetInt64(out var number))
            {
                return number;
            }

            if (value.ValueKind == JsonValueKind.String && long.TryParse(value.GetString(), out number))
            {
                return number;
            }
        }

        return null;
    }

    private static string? TryFindString(JsonElement element, HashSet<string> names)
    {
        if (TryFindValue(element, names, out var value))
        {
            return value.ValueKind == JsonValueKind.String ? value.GetString() : value.GetRawText();
        }

        return null;
    }

    private static bool TryFindValue(JsonElement element, HashSet<string> names, out JsonElement value)
    {
        var queue = new Queue<JsonElement>();
        queue.Enqueue(element);

        while (queue.Count > 0)
        {
            var current = queue.Dequeue();
            switch (current.ValueKind)
            {
                case JsonValueKind.Object:
                    foreach (var property in current.EnumerateObject())
                    {
                        if (names.Contains(property.Name))
                        {
                            value = property.Value;
                            return true;
                        }

                        if (property.Value.ValueKind is JsonValueKind.Object or JsonValueKind.Array)
                        {
                            queue.Enqueue(property.Value);
                        }
                    }
                    break;
                case JsonValueKind.Array:
                    foreach (var item in current.EnumerateArray())
                    {
                        if (item.ValueKind is JsonValueKind.Object or JsonValueKind.Array)
                        {
                            queue.Enqueue(item);
                        }
                    }
                    break;
            }
        }

        value = default;
        return false;
    }

    private static bool? DetermineSuccess(string? status)
    {
        if (string.IsNullOrWhiteSpace(status))
        {
            return null;
        }

        var normalized = status.Trim().ToUpperInvariant();
        if (normalized.Contains("SUCCESS"))
        {
            return true;
        }

        if (normalized.Contains("FAIL") || normalized.Contains("ERROR") || normalized.Contains("CANCEL"))
        {
            return false;
        }

        return null;
    }

    private static string? Trim(string? value, int maxLength)
        => string.IsNullOrWhiteSpace(value) ? null : (value.Length <= maxLength ? value : value[..maxLength]);

    private sealed record WebhookPayload(long? RunId, long? JobId, bool? IsSuccess, string? ErrorMessage);
}

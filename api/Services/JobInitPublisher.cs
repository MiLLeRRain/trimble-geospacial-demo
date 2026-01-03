namespace Trimble.Geospatial.Api.Services;

using System.Diagnostics;
using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Options;
using Trimble.Geospatial.Api.Options;

public interface IJobInitPublisher
{
    Task PublishAsync(JobInitMessage message, CancellationToken cancellationToken);
}

public sealed record JobInitMessage(string JobId, string LandingPath, string SiteId, string IngestRunId);

public sealed class LoggingJobInitPublisher : IJobInitPublisher
{
    private readonly ILogger<LoggingJobInitPublisher> _logger;

    public LoggingJobInitPublisher(ILogger<LoggingJobInitPublisher> logger)
    {
        _logger = logger;
    }

    public Task PublishAsync(JobInitMessage message, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Stub publish job init message {@Message}", message);
        return Task.CompletedTask;
    }
}

public sealed class ServiceBusJobInitPublisher : IJobInitPublisher, IAsyncDisposable
{
    private readonly ServiceBusSender _sender;
    private readonly ILogger<ServiceBusJobInitPublisher> _logger;

    public ServiceBusJobInitPublisher(
        ServiceBusClient client,
        IOptions<ServiceBusOptions> options,
        ILogger<ServiceBusJobInitPublisher> logger)
    {
        var config = options.Value;
        _sender = client.CreateSender(config.QueueName);
        _logger = logger;
    }

    public async Task PublishAsync(JobInitMessage message, CancellationToken cancellationToken)
    {
        var payload = new
        {
            jobId = message.JobId,
            landing_path = message.LandingPath,
            siteId = message.SiteId,
            ingestRunId = message.IngestRunId
        };

        var sbMessage = new ServiceBusMessage(BinaryData.FromString(JsonSerializer.Serialize(payload)))
        {
            ContentType = "application/json",
            Subject = "job_init"
        };

        var correlationId = Activity.Current?.Id;
        if (!string.IsNullOrWhiteSpace(correlationId))
        {
            sbMessage.CorrelationId = correlationId;
        }

        sbMessage.ApplicationProperties["jobId"] = message.JobId;
        sbMessage.ApplicationProperties["siteId"] = message.SiteId;
        sbMessage.ApplicationProperties["ingestRunId"] = message.IngestRunId;

        await _sender.SendMessageAsync(sbMessage, cancellationToken);
        _logger.LogInformation("Published job_init message for JobId={JobId} Queue={Queue}", message.JobId, _sender.EntityPath);
    }

    public async ValueTask DisposeAsync()
    {
        await _sender.DisposeAsync();
    }
}

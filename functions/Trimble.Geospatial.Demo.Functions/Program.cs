using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Trimble.Geospatial.Demo.Functions;
using Trimble.Geospatial.Demo.Functions.Options;
using Trimble.Geospatial.Demo.Functions.Repositories;

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureServices(services =>
    {
        services.AddHttpClient("Databricks");

        services.AddOptions<JobDbOptions>()
            .BindConfiguration("JobDb")
            .Validate(options => !string.IsNullOrWhiteSpace(options.Server), "JobDb__Server must be configured")
            .Validate(options => !string.IsNullOrWhiteSpace(options.Database), "JobDb__Database must be configured")
            .ValidateOnStart();

        services.AddOptions<DatabricksOptions>()
            .Configure<IConfiguration>((options, config) =>
            {
                options.WorkspaceUrl = config["DATABRICKS_WORKSPACE_URL"];
                options.JobId = config["DATABRICKS_JOB_ID"];
                options.Token = config["DATABRICKS_TOKEN"];
            })
            .Validate(options => !string.IsNullOrWhiteSpace(options.WorkspaceUrl), "DATABRICKS_WORKSPACE_URL must be configured")
            .Validate(options => !string.IsNullOrWhiteSpace(options.JobId), "DATABRICKS_JOB_ID must be configured")
            .ValidateOnStart();

        services.AddOptions<ServiceBusOptions>()
            .Configure<IConfiguration>((options, config) =>
            {
                options.Connection = config["SERVICEBUS__CONNECTION"];
                options.QueueName = config["SERVICEBUS__QUEUE_NAME"];
            })
            .Validate(options => !string.IsNullOrWhiteSpace(options.Connection), "SERVICEBUS__CONNECTION must be configured")
            .Validate(options => !string.IsNullOrWhiteSpace(options.QueueName), "SERVICEBUS__QUEUE_NAME must be configured")
            .ValidateOnStart();

        services.AddOptions<WebhookOptions>()
            .Configure<IConfiguration>((options, config) =>
            {
                options.Secret = config["DBX_WEBHOOK_SECRET"];
            })
            .Validate(options => !string.IsNullOrWhiteSpace(options.Secret), "DBX_WEBHOOK_SECRET must be configured")
            .ValidateOnStart();

        services.AddSingleton<JobRepository>();
        services.AddSingleton<DatabricksClient>();
        services.AddSingleton<WebhookSignatureValidator>();
    })
    .Build();

host.Run();

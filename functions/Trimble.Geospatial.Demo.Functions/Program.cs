using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Trimble.Geospatial.Demo.Functions.Options;
using Trimble.Geospatial.Demo.Functions.Repositories;

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureServices(services =>
    {
        services.AddOptions<JobDbOptions>()
            .BindConfiguration("JobDb")
            .Validate(options => !string.IsNullOrWhiteSpace(options.Server), "JobDb__Server must be configured")
            .Validate(options => !string.IsNullOrWhiteSpace(options.Database), "JobDb__Database must be configured")
            .ValidateOnStart();

        services.AddSingleton<JobDbRepository>();
    })
    .Build();

host.Run();

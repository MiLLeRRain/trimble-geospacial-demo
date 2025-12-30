using System.Diagnostics;
using Azure.Core;
using Azure.Identity;
using Microsoft.AspNetCore.Diagnostics;
using Microsoft.Extensions.Options;
using Trimble.Geospatial.Api.Middleware;
using Trimble.Geospatial.Api.Models;
using Trimble.Geospatial.Api.Options;
using Trimble.Geospatial.Api.Services;

var builder = WebApplication.CreateBuilder(args);

var databricksHost = builder.Configuration["DATABRICKS_HOST"];
if (!string.IsNullOrWhiteSpace(databricksHost))
{
    builder.Configuration["Databricks:Host"] = databricksHost;
}

var databricksHttpPath = builder.Configuration["DATABRICKS_HTTP_PATH"];
if (!string.IsNullOrWhiteSpace(databricksHttpPath))
{
    builder.Configuration["Databricks:HttpPath"] = databricksHttpPath;
}

var databricksAadScope = builder.Configuration["DATABRICKS_AAD_SCOPE"];
if (!string.IsNullOrWhiteSpace(databricksAadScope))
{
    builder.Configuration["Databricks:AadScope"] = databricksAadScope;
}

var internalApiKey = builder.Configuration["INTERNAL_API_KEY"];
if (!string.IsNullOrWhiteSpace(internalApiKey))
{
    builder.Configuration["InternalApi:ApiKey"] = internalApiKey;
}

builder.Services.AddControllers();

builder.Services.Configure<DatabricksOptions>(builder.Configuration.GetSection("Databricks"));
builder.Services.Configure<InternalApiOptions>(builder.Configuration.GetSection("InternalApi"));

var tenantId = builder.Configuration["AAD_TENANT_ID"];
var credentialOptions = new DefaultAzureCredentialOptions();
if (!string.IsNullOrWhiteSpace(tenantId))
{
    credentialOptions.TenantId = tenantId;
}

builder.Services.AddSingleton<TokenCredential>(_ => new DefaultAzureCredential(credentialOptions));

builder.Services.AddHttpClient<DatabricksSqlClient>((sp, client) =>
{
    var options = sp.GetRequiredService<IOptions<DatabricksOptions>>().Value;
    client.BaseAddress = options.GetHostUri();
    client.Timeout = TimeSpan.FromSeconds(30);
});

builder.Services.AddScoped<SqlHealthService>();

var app = builder.Build();

app.UseExceptionHandler(errorApp =>
{
    errorApp.Run(async context =>
    {
        var logger = context.RequestServices.GetRequiredService<ILoggerFactory>().CreateLogger("GlobalException");
        var feature = context.Features.Get<IExceptionHandlerFeature>();
        var correlationId = context.GetCorrelationId();
        var exception = feature?.Error;

        if (exception is not null)
        {
            logger.LogError(exception, "Unhandled exception. CorrelationId={CorrelationId}", correlationId);
        }

        context.Response.StatusCode = StatusCodes.Status500InternalServerError;
        context.Response.ContentType = "application/json";

        var payload = ApiError.From("UnhandledException", "An unexpected error occurred.", correlationId);
        await context.Response.WriteAsJsonAsync(payload);
    });
});

app.UseMiddleware<RequestLoggingMiddleware>();
app.UseMiddleware<ApiKeyMiddleware>();

app.MapControllers();

app.Run();

internal static class HttpContextExtensions
{
    public static string GetCorrelationId(this HttpContext context)
    {
        return Activity.Current?.TraceId.ToString() ?? context.TraceIdentifier;
    }
}

using System.Diagnostics;
using Azure.Core;
using Azure.Identity;
using Microsoft.AspNetCore.Diagnostics;
using Microsoft.OpenApi.Models;
using Microsoft.Extensions.Options;
using Trimble.Geospatial.Api.Middleware;
using Trimble.Geospatial.Api.Models;
using Trimble.Geospatial.Api.Options;
using Trimble.Geospatial.Api.Repositories;
using Trimble.Geospatial.Api.Services;
using Trimble.Geospatial.Api.Swagger;

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

var publicApiKey = builder.Configuration["PUBLIC_API_KEY"];
if (!string.IsNullOrWhiteSpace(publicApiKey))
{
    builder.Configuration["PublicApi:ApiKey"] = publicApiKey;
}

builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(options =>
{
    options.SwaggerDoc("v1", new OpenApiInfo
    {
        Title = "Trimble Geospatial Demo API",
        Version = "v1",
        Description = "Public API for pipeline runs, tile statistics, water bodies, and building candidates."
    });

    options.DocumentFilter<SwaggerTagsDocumentFilter>();
    options.OperationFilter<SwaggerExamplesOperationFilter>();
    options.ParameterFilter<SwaggerParameterFilter>();

    var apiKeyScheme = new OpenApiSecurityScheme
    {
        Description = "API key required for all endpoints. Provide the value in the x-api-key header.",
        Name = "x-api-key",
        In = ParameterLocation.Header,
        Type = SecuritySchemeType.ApiKey
    };

    options.AddSecurityDefinition("ApiKey", apiKeyScheme);
    options.AddSecurityRequirement(new OpenApiSecurityRequirement
    {
        [new OpenApiSecurityScheme
            {
                Reference = new OpenApiReference
                {
                    Type = ReferenceType.SecurityScheme,
                    Id = "ApiKey"
                }
            }] = Array.Empty<string>()
    });
});

builder.Services.AddOptions<DatabricksOptions>()
    .Bind(builder.Configuration.GetSection("Databricks"))
    .ValidateDataAnnotations()
    .Validate(options => options.GetHostUri() is not null, "Databricks Host must be a valid absolute URI.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.GetWarehouseId()), "Databricks HttpPath must include a warehouse id.")
    .ValidateOnStart();
builder.Services.Configure<InternalApiOptions>(builder.Configuration.GetSection("InternalApi"));
builder.Services.Configure<PublicApiOptions>(builder.Configuration.GetSection("PublicApi"));

var tenantId = builder.Configuration["AAD_TENANT_ID"] ?? builder.Configuration["AZURE_TENANT_ID"];
var credentialOptions = new DefaultAzureCredentialOptions();
if (!string.IsNullOrWhiteSpace(tenantId))
{
    credentialOptions.TenantId = tenantId;
}

var managedIdentityClientId = builder.Configuration["MANAGED_IDENTITY_CLIENT_ID"];
if (!string.IsNullOrWhiteSpace(managedIdentityClientId))
{
    credentialOptions.ManagedIdentityClientId = managedIdentityClientId;
}

builder.Services.AddSingleton<TokenCredential>(_ => new DefaultAzureCredential(credentialOptions));

builder.Services.AddHttpClient<DatabricksSqlClient>((sp, client) =>
{
    var options = sp.GetRequiredService<IOptions<DatabricksOptions>>().Value;
    client.BaseAddress = options.GetHostUri();
    client.Timeout = TimeSpan.FromSeconds(30);
});

builder.Services.AddScoped<SqlHealthService>();
builder.Services.AddScoped<DatabricksSqlQueryExecutor>();
builder.Services.AddScoped<PipelineRunRepository>();
builder.Services.AddScoped<TileStatsRepository>();
builder.Services.AddScoped<WaterBodyRepository>();
builder.Services.AddScoped<BuildingCandidateRepository>();

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

app.UseSwagger();
app.UseSwaggerUI(options =>
{
    options.SwaggerEndpoint("/swagger/v1/swagger.json", "Trimble Geospatial API v1");
    options.DisplayRequestDuration();
});

app.MapControllers();

app.Run();

internal static class HttpContextExtensions
{
    public static string GetCorrelationId(this HttpContext context)
    {
        return Activity.Current?.TraceId.ToString() ?? context.TraceIdentifier;
    }
}

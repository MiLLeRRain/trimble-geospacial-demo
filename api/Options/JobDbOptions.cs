using Microsoft.Data.SqlClient;

namespace Trimble.Geospatial.Api.Options;

public sealed class JobDbOptions
{
    public string? Server { get; set; }
    public string? Database { get; set; }

    // Authentication requirements:
    // - In App Service, Active Directory Default should pick up Managed Identity.
    // - Locally, it will typically use VS/CLI sign-in.
    public string GetConnectionString()
    {
        if (string.IsNullOrWhiteSpace(Server))
        {
            throw new InvalidOperationException("JobDb:Server is not configured (set JobDb__Server).");
        }

        if (string.IsNullOrWhiteSpace(Database))
        {
            throw new InvalidOperationException("JobDb:Database is not configured (set JobDb__Database).");
        }

        var builder = new SqlConnectionStringBuilder
        {
            DataSource = Server,
            InitialCatalog = Database,
            Encrypt = true,
            TrustServerCertificate = false,
            ConnectTimeout = 30,
        };

        // Keep this exactly as requested.
        builder["Authentication"] = "Active Directory Default";

        return builder.ConnectionString;
    }
}

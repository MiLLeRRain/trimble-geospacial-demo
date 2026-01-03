using Microsoft.Data.SqlClient;

namespace Trimble.Geospatial.Demo.Functions.Options;

public sealed class JobDbOptions
{
    public string? Server { get; set; }
    public string? Database { get; set; }

    public string GetConnectionString()
    {
        if (string.IsNullOrWhiteSpace(Server))
        {
            throw new InvalidOperationException("JobDb__Server is not configured");
        }

        if (string.IsNullOrWhiteSpace(Database))
        {
            throw new InvalidOperationException("JobDb__Database is not configured");
        }

        var builder = new SqlConnectionStringBuilder
        {
            DataSource = Server,
            InitialCatalog = Database,
            Encrypt = true,
            TrustServerCertificate = false,
            ConnectTimeout = 30,
        };

        // Requested: use AAD default (Managed Identity in Azure, VS/CLI locally).
        builder["Authentication"] = "Active Directory Default";

        return builder.ConnectionString;
    }
}

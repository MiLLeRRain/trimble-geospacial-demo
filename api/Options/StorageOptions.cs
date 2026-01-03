using System.ComponentModel.DataAnnotations;

namespace Trimble.Geospatial.Api.Options;

public sealed class StorageOptions
{
    [Required]
    public string? AccountName { get; set; }

    [Required]
    public string? ContainerName { get; set; }

    // Optional override, e.g. https://account.dfs.core.windows.net
    public string? Endpoint { get; set; }

    // Optional fallback when User Delegation SAS is unavailable.
    public string? AccountKey { get; set; }

    public Uri GetServiceEndpoint()
    {
        var endpoint = string.IsNullOrWhiteSpace(Endpoint)
            ? $"https://{AccountName}.blob.core.windows.net"
            : Endpoint;

        if (!Uri.TryCreate(endpoint, UriKind.Absolute, out var uri))
        {
            throw new InvalidOperationException("Storage endpoint is invalid.");
        }

        return uri;
    }

    public Uri GetContainerUri()
    {
        var root = GetServiceEndpoint().AbsoluteUri.TrimEnd('/');
        return new Uri($"{root}/{ContainerName}");
    }
}

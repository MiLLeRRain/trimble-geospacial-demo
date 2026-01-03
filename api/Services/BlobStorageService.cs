using Azure;
using Azure.Core;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Sas;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Options;
using Trimble.Geospatial.Api.Options;

namespace Trimble.Geospatial.Api.Services;

public interface IBlobStorageService
{
    Task<BlobSasResult> GetWriteSasAsync(string blobPath, string contentType, CancellationToken cancellationToken);
    Task<BlobMetadata?> TryGetBlobAsync(Uri blobUri, CancellationToken cancellationToken);
}

public sealed class BlobStorageService : IBlobStorageService
{
    private readonly BlobServiceClient _serviceClient;
    private readonly BlobContainerClient _containerClient;
    private readonly StorageSharedKeyCredential? _sharedKeyCredential;
    private readonly TokenCredential _tokenCredential;
    private readonly ILogger<BlobStorageService> _logger;

    public BlobStorageService(
        IOptions<StorageOptions> options,
        TokenCredential tokenCredential,
        ILogger<BlobStorageService> logger)
    {
        var config = options.Value;
        _tokenCredential = tokenCredential;
        _logger = logger;

        _sharedKeyCredential = string.IsNullOrWhiteSpace(config.AccountKey)
            ? null
            : new StorageSharedKeyCredential(config.AccountName, config.AccountKey);

        _serviceClient = _sharedKeyCredential is not null
            ? new BlobServiceClient(config.GetServiceEndpoint(), _sharedKeyCredential)
            : new BlobServiceClient(config.GetServiceEndpoint(), tokenCredential);

        _containerClient = _serviceClient.GetBlobContainerClient(config.ContainerName);
    }

    public async Task<BlobSasResult> GetWriteSasAsync(string blobPath, string contentType, CancellationToken cancellationToken)
    {
        var now = DateTimeOffset.UtcNow;
        var expiresOn = now.AddMinutes(15);

        var sasBuilder = new BlobSasBuilder
        {
            BlobContainerName = _containerClient.Name,
            BlobName = blobPath,
            Resource = "b",
            StartsOn = now.AddMinutes(-5),
            ExpiresOn = expiresOn
        };

        sasBuilder.SetPermissions(BlobSasPermissions.Create | BlobSasPermissions.Write | BlobSasPermissions.Add);
        sasBuilder.ContentType = contentType;

        var blobClient = _containerClient.GetBlobClient(blobPath);
        var sasToken = await BuildSasTokenAsync(sasBuilder, cancellationToken);
        var sasUri = new Uri($"{blobClient.Uri}?{sasToken}");

        return new BlobSasResult(blobClient.Uri, sasUri, expiresOn);
    }

    public async Task<BlobMetadata?> TryGetBlobAsync(Uri blobUri, CancellationToken cancellationToken)
    {
        var client = CreateBlobClient(blobUri);
        try
        {
            var props = await client.GetPropertiesAsync(cancellationToken: cancellationToken);
            return new BlobMetadata(props.Value.ETag.ToString(), props.Value.ContentLength);
        }
        catch (RequestFailedException ex) when (ex.Status == StatusCodes.Status404NotFound)
        {
            _logger.LogWarning("Blob not found at {BlobUri}", blobUri);
            return null;
        }
    }

    private BlobClient CreateBlobClient(Uri uri)
    {
        if (_sharedKeyCredential is not null)
        {
            return new BlobClient(uri, _sharedKeyCredential);
        }

        return new BlobClient(uri, _tokenCredential);
    }

    private async Task<string> BuildSasTokenAsync(BlobSasBuilder builder, CancellationToken cancellationToken)
    {
        if (_sharedKeyCredential is not null)
        {
            return builder.ToSasQueryParameters(_sharedKeyCredential).ToString();
        }

        var userDelegationKey = await _serviceClient.GetUserDelegationKeyAsync(builder.StartsOn, builder.ExpiresOn, cancellationToken: cancellationToken);
        return builder.ToSasQueryParameters(userDelegationKey, _serviceClient.AccountName).ToString();
    }
}

public sealed record BlobSasResult(Uri BlobUri, Uri SasUri, DateTimeOffset ExpiresOn);

public sealed record BlobMetadata(string ETag, long ContentLength);

using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using Trimble.Geospatial.Api.Models;
using Trimble.Geospatial.Api.Options;
using Trimble.Geospatial.Api.Repositories;
using Trimble.Geospatial.Api.Services;

namespace Trimble.Geospatial.Api.Controllers;

[ApiController]
[Route("v1/uploads")]
public sealed class UploadsController : ControllerBase
{
    private readonly UploadRepository _repository;
    private readonly IBlobStorageService _storage;
    private readonly IJobInitPublisher _publisher;
    private readonly StorageOptions _storageOptions;
    private readonly ILogger<UploadsController> _logger;

    public UploadsController(
        UploadRepository repository,
        IBlobStorageService storage,
        IJobInitPublisher publisher,
        IOptions<StorageOptions> storageOptions,
        ILogger<UploadsController> logger)
    {
        _repository = repository;
        _storage = storage;
        _publisher = publisher;
        _storageOptions = storageOptions.Value;
        _logger = logger;
    }

    [HttpPost]
    public async Task<IActionResult> CreateUploadSession([FromBody] UploadSessionRequest request, CancellationToken cancellationToken)
    {
        var correlationId = HttpContext.GetCorrelationId();
        var idempotencyKey = Request.Headers["Idempotency-Key"].FirstOrDefault();
        if (string.IsNullOrWhiteSpace(idempotencyKey))
        {
            return BadRequest(ApiError.From("MissingIdempotencyKey", "Idempotency-Key header is required.", correlationId));
        }

        var existing = await _repository.GetByIdempotencyKeyAsync(idempotencyKey, cancellationToken);

        UploadSessionRecord session;
        string blobPath;

        if (existing is not null)
        {
            session = existing;
            blobPath = GetBlobPath(existing.BlobUri);
            _logger.LogInformation("Reusing upload session for idempotency key {IdempotencyKey}", idempotencyKey);
        }
        else
        {
            var jobId = Guid.NewGuid().ToString("N");
            var uploadId = Guid.NewGuid().ToString("N");
            blobPath = $"laz/siteId={request.SiteId}/ingestRunId={jobId}/{request.FileName}";
            var blobUri = new Uri(_storageOptions.GetContainerUri(), blobPath).ToString();

            var insert = new UploadSessionInsert(
                JobId: jobId,
                UploadId: uploadId,
                SiteId: request.SiteId,
                FileName: request.FileName,
                ContentType: request.ContentType,
                ContentLength: request.ContentLength,
                BlobUri: blobUri,
                LandingPath: blobUri,
                IdempotencyKey: idempotencyKey,
                TargetIngestRunId: jobId,
                CorrelationId: correlationId);

            session = await _repository.InsertUploadSessionAsync(insert, cancellationToken);
        }

        var sas = await _storage.GetWriteSasAsync(blobPath, request.ContentType, cancellationToken);

        var response = new UploadSessionResponse
        {
            JobId = session.JobId,
            UploadId = session.UploadId,
            LandingPath = session.LandingPath,
            UploadUrl = sas.SasUri.ToString(),
            ExpiresAtUtc = sas.ExpiresOn
        };

        return Ok(response);
    }

    [HttpPost("{uploadId}/complete")]
    public async Task<IActionResult> CompleteUpload([FromRoute] string uploadId, [FromBody] UploadCompleteRequest? request, CancellationToken cancellationToken)
    {
        var correlationId = HttpContext.GetCorrelationId();
        var upload = await _repository.GetUploadWithJobAsync(uploadId, cancellationToken);
        if (upload is null)
        {
            return NotFound(ApiError.From("UploadNotFound", "Upload session not found.", correlationId));
        }

        var blobUri = new Uri(upload.BlobUri);
        var blobMetadata = await _storage.TryGetBlobAsync(blobUri, cancellationToken);
        if (blobMetadata is null)
        {
            return Conflict(ApiError.From("UploadIncomplete", "blob not found / upload not complete", correlationId));
        }

        var etag = request?.ETag ?? blobMetadata.ETag;
        var sizeBytes = request?.SizeBytes ?? blobMetadata.ContentLength;

        var completion = await _repository.CompleteUploadAsync(uploadId, etag, sizeBytes, cancellationToken);
        if (completion is null)
        {
            return NotFound(ApiError.From("UploadNotFound", "Upload session not found.", correlationId));
        }

        var message = new JobInitMessage(
            completion.JobId,
            completion.LandingPath,
            completion.SiteId,
            completion.TargetIngestRunId);
        await _publisher.PublishAsync(message, cancellationToken);

        var response = new UploadStatusResponse
        {
            JobId = completion.JobId,
            Status = completion.JobStatus,
            LandingPath = completion.LandingPath,
            SiteId = completion.SiteId,
            IngestRunId = completion.TargetIngestRunId
        };

        return Ok(response);
    }

    private string GetBlobPath(string blobUri)
    {
        if (string.IsNullOrWhiteSpace(blobUri))
        {
            throw new InvalidOperationException("Stored blob uri is missing.");
        }

        var containerUri = _storageOptions.GetContainerUri();
        var uri = new Uri(blobUri);
        if (!uri.AbsolutePath.StartsWith(containerUri.AbsolutePath, StringComparison.OrdinalIgnoreCase))
        {
            return uri.AbsolutePath.TrimStart('/');
        }

        var relative = uri.AbsolutePath.Substring(containerUri.AbsolutePath.Length).TrimStart('/');
        return relative;
    }
}


/*
    Create Jobs tables for managing data ingestion jobs.
*/
CREATE TABLE dbo.Jobs (
    JobId               nvarchar(64)  NOT NULL PRIMARY KEY,  -- GUID/ULID
    SiteId              nvarchar(128) NOT NULL,
    Status              nvarchar(32)  NOT NULL,               -- CREATED/QUEUED/DBX_TRIGGERED/PROCESSING/SUCCEEDED/FAILED
    TargetIngestRunId   nvarchar(64)  NOT NULL,               -- can be multiple to 1 JobId
    DatabricksRunId     bigint        NULL,
    LandingPath         nvarchar(1024) NULL,
    StagingPath         nvarchar(1024) NULL,

    IdempotencyKey      nvarchar(128) NULL,                   -- for idempotent POST /uploads
    CreatedBy           nvarchar(256) NULL,
    CorrelationId       nvarchar(64)  NULL,

    CreatedAtUtc        datetime2(3)  NOT NULL DEFAULT SYSUTCDATETIME(),
    UpdatedAtUtc        datetime2(3)  NOT NULL DEFAULT SYSUTCDATETIME(),

    ErrorCode           nvarchar(64)  NULL,
    ErrorMessage        nvarchar(2048) NULL
);



-- Make IdempotencyKey unique when it has a value (to avoid creating duplicate Jobs)
CREATE UNIQUE INDEX UX_Jobs_IdempotencyKey
ON dbo.Jobs (IdempotencyKey)
WHERE IdempotencyKey IS NOT NULL;

CREATE INDEX IX_Jobs_SiteId_CreatedAt
ON dbo.Jobs (SiteId, CreatedAtUtc DESC);


/*
   Create Uploads table for managing file uploads associated with Jobs.
*/
CREATE TABLE dbo.Uploads (
    UploadId            nvarchar(64)  NOT NULL PRIMARY KEY,
    JobId               nvarchar(64)  NOT NULL,
    FileName            nvarchar(256) NULL,
    ContentType         nvarchar(128) NULL,
    SizeBytes           bigint        NULL,
    BlobUri             nvarchar(1024) NULL,
    BlobETag            nvarchar(128) NULL,

    UploadStatus        nvarchar(32)  NOT NULL,               -- URL_ISSUED/UPLOADED/COMPLETED
    CreatedAtUtc        datetime2(3)  NOT NULL DEFAULT SYSUTCDATETIME(),
    CompletedAtUtc      datetime2(3)  NULL,

    CONSTRAINT FK_Uploads_Jobs FOREIGN KEY (JobId) REFERENCES dbo.Jobs(JobId)
);

-- Common query: find job by uploadId
CREATE INDEX IX_Uploads_JobId
ON dbo.Uploads (JobId);
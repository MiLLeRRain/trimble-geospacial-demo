/* SQL Server table definition for Jobs */
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
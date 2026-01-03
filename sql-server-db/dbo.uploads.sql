/* SQL Server table definition for Uploads */
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
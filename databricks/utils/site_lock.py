# utils/site_lock.py
from datetime import timedelta
from pyspark.sql import functions as F

DEFAULT_LOCK_TABLE = "site_locks"   # expects current catalog/schema


def _current_timestamp(spark):
    return spark.sql("SELECT current_timestamp() AS ts").first()["ts"]


def acquire_site_lock(
    spark,
    site_id: str,
    locked_by: str,
    ttl_minutes: int = 60,
    lock_table: str = DEFAULT_LOCK_TABLE,
):
    """
    Acquire an exclusive lock for a siteId.
    Fails fast if the site is already locked by another run.

    Lock semantics:
    - One row per siteId
    - Lock can be taken if:
        - siteId does not exist
        - OR existing lock has expired (expiresAt < now)

    Args:
        spark: SparkSession
        site_id: site identifier
        locked_by: jobRunId / notebook id / user
        ttl_minutes: lock TTL
        lock_table: Delta table name (must exist)

    Raises:
        RuntimeError if lock cannot be acquired
    """

    now = _current_timestamp(spark)
    expires_at = now + timedelta(minutes=ttl_minutes)

    spark.sql(f"""
    MERGE INTO {lock_table} AS t
    USING (
      SELECT
        '{site_id}' AS siteId,
        '{locked_by}' AS lockedBy,
        TIMESTAMP('{now}') AS lockedAt,
        TIMESTAMP('{expires_at}') AS expiresAt
    ) AS s
    ON t.siteId = s.siteId
    WHEN MATCHED AND t.expiresAt < current_timestamp() THEN
      UPDATE SET
        lockedBy  = s.lockedBy,
        lockedAt  = s.lockedAt,
        expiresAt = s.expiresAt
    WHEN NOT MATCHED THEN
      INSERT (siteId, lockedBy, lockedAt, expiresAt)
      VALUES (s.siteId, s.lockedBy, s.lockedAt, s.expiresAt)
    """)

    owner = (
        spark.table(lock_table)
             .filter(F.col("siteId") == site_id)
             .select("lockedBy")
             .first()["lockedBy"]
    )

    if owner != locked_by:
        raise RuntimeError(
            f"❌ Failed to acquire lock for siteId='{site_id}'. "
            f"Currently locked by '{owner}'."
        )

    print(f"🔒 Lock acquired: siteId='{site_id}', lockedBy='{locked_by}'")


def release_site_lock(
    spark,
    site_id: str,
    locked_by: str,
    lock_table: str = DEFAULT_LOCK_TABLE,
):
    """
    Release a site lock if owned by this run.
    Safe to call multiple times.

    Args:
        spark: SparkSession
        site_id: site identifier
        locked_by: jobRunId / notebook id / user
        lock_table: Delta table name
    """

    spark.sql(f"""
    DELETE FROM {lock_table}
    WHERE siteId = '{site_id}'
      AND lockedBy = '{locked_by}'
    """)

    print(f"🔓 Lock released: siteId='{site_id}', lockedBy='{locked_by}'")

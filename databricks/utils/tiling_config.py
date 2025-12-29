# utils/tiling_config.py
from pyspark.sql import functions as F

def resolve_tiling_config(
    spark,
    *,
    site_id: str,
    ingest_run_id: str,
    use_control_table: bool,
    allow_fallback: bool,
    control_path: str,
    defaults: dict
) -> dict:
    """
    Resolve spatial tiling configuration with priority:
    1) control table
    2) code defaults

    Returns a dict with resolved params + metadata.
    """

    # ---- start with defaults ----
    config = {
        "tileSizeM": defaults["tileSizeM"],
        "hotTileThreshold": defaults["hotTileThreshold"],
        "targetPointsPerBucket": defaults["targetPointsPerBucket"],
        "saltBuckets": defaults["saltBuckets"],
        "enableSalt": defaults["enableSalt"],
        "paramSource": "code-defaults"
    }

    if not use_control_table:
        return config

    try:
        df = (
            spark.read.format("delta").load(control_path)
            .filter(
                (F.col("siteId") == site_id) &
                (F.col("ingestRunId") == ingest_run_id)
            )
            .orderBy(F.col("computedAt").desc())
            .limit(1)
        )

        rows = df.collect()
        if not rows:
            if not allow_fallback:
                raise ValueError("No control-table entry found.")
            return config

        r = rows[0]

        config.update({
            "tileSizeM": float(r["tileSizeM"]),
            "hotTileThreshold": int(r["hotTileThreshold"]),
            "targetPointsPerBucket": int(r["targetPointsPerBucket"]),
            "saltBuckets": int(r["saltBuckets"]),
            # data-driven enable/disable
            "enableSalt": int(r["maxTilePoints"]) >= int(r["targetPointsPerBucket"]),
            "paramSource": "control-table"
        })

        return config

    except Exception as e:
        if not allow_fallback:
            raise
        print("⚠️ Failed to resolve config from control table, fallback to defaults.")
        print("Reason:", str(e)[:200])
        return config


def print_config(config: dict):
    print("=== Spatial Tiling Config ===")
    for k, v in config.items():
        print(f"{k}: {v}")
    print("============================")

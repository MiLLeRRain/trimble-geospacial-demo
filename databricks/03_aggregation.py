# 03_aggregation.py
from pyspark.sql import functions as F

# =========================
# PATHS
# =========================
PROCESSED_PATH = "abfss://processed@trimblegeospatialdemo.dfs.core.windows.net/points_tiled"
AGG_TILE_STATS_PATH = "abfss://aggregated@trimblegeospatialdemo.dfs.core.windows.net/tile_stats"
AGG_TILE_WATER_MASK_PATH = "abfss://aggregated@trimblegeospatialdemo.dfs.core.windows.net/tile_water_mask"

CONTROL_PATH = "abfss://aggregated@trimblegeospatialdemo.dfs.core.windows.net/control/tiling_params"

# =========================
# PARAMS
# =========================
SITE_ID = "wellington_cbd"
WATER_CLASS = 9

# Optional: use control table to confirm tileSize, etc.
USE_CONTROL_TABLE = True
ALLOW_FALLBACK = True

DEFAULTS = {
    "tileSizeM": 25.0,             # only used for logging/consistency
    "hotTileThreshold": 100_000,
    "targetPointsPerBucket": 600_000,
    "saltBuckets": 16,
    "enableSalt": False
}

# =========================
# 1) Read processed + discover ingestRunId
# =========================
df = (
    spark.read.format("delta").load(PROCESSED_PATH)
    .filter(F.col("siteId") == SITE_ID)
)

ingest_ids = [r["ingestRunId"] for r in df.select("ingestRunId").distinct().collect()]
if len(ingest_ids) != 1:
    raise ValueError(f"Expected exactly one ingestRunId in processed for site={SITE_ID}, found {ingest_ids}")

INGEST_RUN_ID = ingest_ids[0]
df = df.filter(F.col("ingestRunId") == INGEST_RUN_ID)

print("Using ingestRunId =", INGEST_RUN_ID)

# =========================
# 2) Load control params (optional, mainly for audit/logging)
# =========================
tileSizeM = DEFAULTS["tileSizeM"]
param_source = "code-defaults"

if USE_CONTROL_TABLE:
    try:
        p = (
            spark.read.format("delta").load(CONTROL_PATH)
            .filter((F.col("siteId")==SITE_ID) & (F.col("ingestRunId")==INGEST_RUN_ID))
            .orderBy(F.col("computedAt").desc())
            .limit(1)
            .collect()
        )
        if p:
            tileSizeM = float(p[0]["tileSizeM"])
            param_source = "control-table"
        else:
            if not ALLOW_FALLBACK:
                raise ValueError("No control params found.")
    except Exception as e:
        if not ALLOW_FALLBACK:
            raise
        print("⚠️ Failed to read control table, continue with defaults. Reason:", str(e)[:200])

print(f"Aggregation tileSizeM={tileSizeM} (source={param_source})")

# =========================
# 3) Compute tile_stats (1 tile = 1 row)
# =========================
# NOTE: avgZ uses sumZ/pointCount (merge-safe)
tile_stats = (
    df.groupBy("siteId","ingestRunId","tileX","tileY")
    .agg(
        F.count("*").alias("pointCount"),
        F.sum(F.when(F.col("classification") == WATER_CLASS, 1).otherwise(0)).alias("waterCount"),

        # bbox per tile
        F.min("x").alias("minX"),
        F.max("x").alias("maxX"),
        F.min("y").alias("minY"),
        F.max("y").alias("maxY"),

        # vertical range
        F.min("z").alias("minZ"),
        F.max("z").alias("maxZ"),

        # merge-safe average
        F.sum(F.col("z")).alias("sumZ")
    )
    .withColumn("avgZ", F.col("sumZ") / F.col("pointCount"))
    .withColumn("waterRatio", F.col("waterCount") / F.col("pointCount"))
    .drop("sumZ")
    .withColumn("tileSizeM", F.lit(tileSizeM))
)

# quick sanity
tile_stats.orderBy(F.col("pointCount").desc()).show(20, truncate=False)

# =========================
# 4) Write aggregated tile_stats
# =========================
(
    tile_stats
    .write.format("delta")
    .mode("append")
    .partitionBy("siteId","ingestRunId")
    .save(AGG_TILE_STATS_PATH)
)

print("✅ Aggregated tile_stats written:", AGG_TILE_STATS_PATH)

# =========================
# 5) Optional: write a water mask table (for pruning later)
# =========================
# Example rule: mark tiles as "mostly water" if waterRatio >= 0.6
tile_water_mask = (
    tile_stats
    .select("siteId","ingestRunId","tileX","tileY","waterRatio","minX","minY","maxX","maxY","tileSizeM")
    .withColumn("isMostlyWater", (F.col("waterRatio") >= F.lit(0.6)).cast("int"))
    .filter(F.col("isMostlyWater") == 1)
)

# Only write if there are any water tiles (avoid empty writes)
if tile_water_mask.limit(1).count() > 0:
    (
        tile_water_mask
        .write.format("delta")
        .mode("append")
        .partitionBy("siteId","ingestRunId")
        .save(AGG_TILE_WATER_MASK_PATH)
    )
    print("✅ Aggregated tile_water_mask written:", AGG_TILE_WATER_MASK_PATH)
else:
    print("ℹ️ No mostly-water tiles detected; skipping tile_water_mask write.")


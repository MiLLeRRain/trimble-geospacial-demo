# 02_spatial_tiling.py
from pyspark.sql import functions as F

# Set Unity Catalog context
spark.sql("USE CATALOG main")
spark.sql("USE SCHEMA demo")

# ==================================================
# CONFIG SWITCHES
# ==================================================
USE_CONTROL_TABLE = False      # ← False to avoid reading control table
ALLOW_FALLBACK = True         # ← control table fallback

# ==================================================
# PATHS & TABLE NAMES
# ==================================================
RAW_PATH = "abfss://raw@trimblegeospatialdemo.dfs.core.windows.net/points"
PROCESSED_PATH = "abfss://processed@trimblegeospatialdemo.dfs.core.windows.net/points_tiled"
CONTROL_PATH = "abfss://aggregated@trimblegeospatialdemo.dfs.core.windows.net/control/tiling_params"

# Unity Catalog table names
PROCESSED_TABLE = "demo_processed_points_tiled"  # main.demo.demo_processed_points_tiled
CONTROL_TABLE = "demo_control_tiling_params"     # main.demo.demo_control_tiling_params

SITE_ID = "wellington_cbd"

# ==================================================
# CODE DEFAULTS (SAFE BASELINE)
# ==================================================
DEFAULT_TILE_SIZE_M = 25.0
DEFAULT_HOT_TILE_THRESHOLD = 100_000
DEFAULT_TARGET_POINTS_PER_BUCKET = 600_000
DEFAULT_SALT_BUCKETS = 16
DEFAULT_ENABLE_SALT = False   # conservative default

# ==================================================
# 1) Load raw + discover ingestRunId
# ==================================================
df_raw = (
    spark.read.format("delta").load(RAW_PATH)
    .filter(F.col("siteId") == SITE_ID)
)

ingest_ids = [r["ingestRunId"] for r in df_raw.select("ingestRunId").distinct().collect()]
if len(ingest_ids) != 1:
    raise ValueError(f"Expected exactly one ingestRunId, found {ingest_ids}")

INGEST_RUN_ID = ingest_ids[0]
df_raw = df_raw.filter(F.col("ingestRunId") == INGEST_RUN_ID)

print("Using ingestRunId =", INGEST_RUN_ID)

# ==================================================
# 2) Resolve parameters (override → control → default)
# ==================================================
param_source = "code-defaults"

TILE_SIZE_M = DEFAULT_TILE_SIZE_M
HOT_TILE_THRESHOLD = DEFAULT_HOT_TILE_THRESHOLD
TARGET_POINTS_PER_BUCKET = DEFAULT_TARGET_POINTS_PER_BUCKET
SALT_BUCKETS = DEFAULT_SALT_BUCKETS
ENABLE_SALT = DEFAULT_ENABLE_SALT

if USE_CONTROL_TABLE:
    try:
        # Read from Unity Catalog table if it exists
        params = (
            spark.table(CONTROL_TABLE)
            .filter((F.col("siteId")==SITE_ID) & (F.col("ingestRunId")==INGEST_RUN_ID))
            .orderBy(F.col("computedAt").desc())
            .limit(1)
            .collect()
        )

        if params:
            p = params[0]
            TILE_SIZE_M = float(p["tileSizeM"])
            HOT_TILE_THRESHOLD = int(p["hotTileThreshold"])
            TARGET_POINTS_PER_BUCKET = int(p["targetPointsPerBucket"])
            SALT_BUCKETS = int(p["saltBuckets"])

            # data-driven decision
            ENABLE_SALT = int(p["maxTilePoints"]) >= TARGET_POINTS_PER_BUCKET

            param_source = "control-table"
        else:
            if not ALLOW_FALLBACK:
                raise ValueError("No control table entry found and fallback disabled.")
            print("⚠️ No control params found; falling back to code defaults.")

    except Exception as e:
        if not ALLOW_FALLBACK:
            raise
        print("⚠️ Failed to read control table, falling back to code defaults.")
        print("Reason:", str(e)[:200])

# ==================================================
# 3) Print final resolved parameters (AUDIT POINT)
# ==================================================
print("=== Spatial Tiling Parameters ===")
print("Source:", param_source)
print("TILE_SIZE_M =", TILE_SIZE_M)
print("HOT_TILE_THRESHOLD =", HOT_TILE_THRESHOLD)
print("TARGET_POINTS_PER_BUCKET =", TARGET_POINTS_PER_BUCKET)
print("SALT_BUCKETS =", SALT_BUCKETS)
print("ENABLE_SALT =", ENABLE_SALT)
print("=================================")

# ==================================================
# 4) Compute tiles
# ==================================================
bbox = df_raw.agg(F.min("x"), F.min("y")).first()
originX, originY = float(bbox[0]), float(bbox[1])

df_tiled = (
    df_raw
    .withColumn("tileX", F.floor((F.col("x") - originX) / TILE_SIZE_M).cast("int"))
    .withColumn("tileY", F.floor((F.col("y") - originY) / TILE_SIZE_M).cast("int"))
)

# ==================================================
# 5) Apply salt only if enabled
# ==================================================
if ENABLE_SALT:
    tile_counts = (
        df_tiled.groupBy("tileX","tileY")
        .agg(F.count("*").alias("pointCount"))
    )

    hot_keys = (
        tile_counts.filter(F.col("pointCount") >= HOT_TILE_THRESHOLD)
        .select("tileX","tileY")
        .distinct()
    )

    hot_keys_b = F.broadcast(hot_keys)

    df_hot = (
        df_tiled.join(hot_keys_b, ["tileX","tileY"], "left_semi")
        .withColumn("tileSalt", F.pmod(F.hash("x","y","z"), F.lit(SALT_BUCKETS)))
        .withColumn("isHotTile", F.lit(1))
    )

    df_non_hot = (
        df_tiled.join(hot_keys_b, ["tileX","tileY"], "left_anti")
        .withColumn("tileSalt", F.lit(0))
        .withColumn("isHotTile", F.lit(0))
    )

    df_processed = df_hot.unionByName(df_non_hot)
else:
    df_processed = (
        df_tiled
        .withColumn("tileSalt", F.lit(0))
        .withColumn("isHotTile", F.lit(0))
    )

# ==================================================
# 6) Write to Unity Catalog using saveAsTable
# ==================================================
print(f"\n=== Writing to Unity Catalog table: {PROCESSED_TABLE} ===")

(
    df_processed
    .write
    .format("delta")
    .mode("append")
    .option("path", PROCESSED_PATH)  # Specify external location
    .partitionBy("siteId", "ingestRunId")
    .saveAsTable(PROCESSED_TABLE)
)

print(f"✅ Data written and table registered in Unity Catalog")
print(f"   - Table: main.demo.{PROCESSED_TABLE}")
print(f"   - Location: {PROCESSED_PATH}")

# Verify table registration
print("\n=== Verify table ===")
result = spark.sql(f"SELECT COUNT(*) as total_rows FROM {PROCESSED_TABLE}")
result.show()

print("\n✅ Complete!")
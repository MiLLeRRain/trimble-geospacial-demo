# 02A_benchmark_tiling_params.py
from pyspark.sql import functions as F, Row
import time
from datetime import datetime

# Set Unity Catalog context
spark.sql("USE CATALOG main")
spark.sql("USE SCHEMA demo")

# ===== Configuration =====
RAW_PATH = "abfss://raw@trimblegeospatialdemo.dfs.core.windows.net/points"
CONTROL_PATH = "abfss://aggregated@trimblegeospatialdemo.dfs.core.windows.net/control/tiling_params"

# Unity Catalog table name
CONTROL_TABLE = "demo_control_tiling_params"  # main.demo.demo_control_tiling_params

SITE_ID = "wellington_cbd"
TILE_SIZE_M = 25.0
WATER_CLASS = 9

# Benchmark knobs
CANDIDATE_TARGETS = [50_000, 100_000, 150_000, 250_000, 400_000, 600_000]
DESIRED_RUNTIME_SEC = 60
ACCEPTABLE_RUNTIME_SEC = 120
MAX_BUCKETS_CAP = 64

# --------------------------------------------------
# 1) Load raw + discover ingestRunId
# --------------------------------------------------
df_raw = (
    spark.read.format("delta").load(RAW_PATH)
    .filter(F.col("siteId") == SITE_ID)
    .select("x","y","z","classification","siteId","ingestRunId")
)

ingest_run_ids = [r["ingestRunId"] for r in df_raw.select("ingestRunId").distinct().collect()]
if len(ingest_run_ids) != 1:
    raise ValueError(f"Expected exactly one ingestRunId, found: {ingest_run_ids}")

INGEST_RUN_ID = ingest_run_ids[0]
print("Using ingestRunId =", INGEST_RUN_ID)

df_raw = df_raw.filter(F.col("ingestRunId") == INGEST_RUN_ID)

# --------------------------------------------------
# 2) Compute tiling
# --------------------------------------------------
bbox = df_raw.agg(F.min("x").alias("minX"), F.min("y").alias("minY")).first()
originX, originY = float(bbox["minX"]), float(bbox["minY"])

df_tiled = (
    df_raw
    .withColumn("tileX", F.floor((F.col("x") - F.lit(originX)) / TILE_SIZE_M).cast("int"))
    .withColumn("tileY", F.floor((F.col("y") - F.lit(originY)) / TILE_SIZE_M).cast("int"))
)

# --------------------------------------------------
# 3) Tile distribution → HOT_TILE_THRESHOLD
# --------------------------------------------------
tile_counts = (
    df_tiled.groupBy("tileX","tileY")
    .agg(F.count("*").alias("pointCount"))
).cache()

tile_counts.count()

qs = tile_counts.approxQuantile("pointCount", [0.90, 0.95, 0.99, 0.995], 0.01)
p90, p95, p99, p995 = qs
max_points = tile_counts.agg(F.max("pointCount")).first()[0]

HOT_TILE_THRESHOLD = int(max(100_000, p99))
hot_tiles = tile_counts.filter(F.col("pointCount") >= HOT_TILE_THRESHOLD).count()

print(f"Tile distribution: p90={p90}, p95={p95}, p99={p99}, max={max_points}")
print(f"Recommended HOT_TILE_THRESHOLD = {HOT_TILE_THRESHOLD}, hotTiles={hot_tiles}")

# --------------------------------------------------
# 4) Benchmark hottest tile → targetPointsPerBucket
# --------------------------------------------------
hot_tile = tile_counts.orderBy(F.col("pointCount").desc()).first()
hot_x, hot_y, hot_count = hot_tile["tileX"], hot_tile["tileY"], hot_tile["pointCount"]

df_hot = (
    df_tiled
    .filter((F.col("tileX")==hot_x) & (F.col("tileY")==hot_y))
    .cache()
)
df_hot.count()

def run_benchmark(target):
    buckets = max(1, min(MAX_BUCKETS_CAP, int((hot_count + target - 1) // target)))
    t0 = time.time()

    df_salted = df_hot.withColumn(
        "tileSalt",
        F.pmod(F.hash("x","y","z"), F.lit(buckets))
    )

    df_salted.groupBy("tileSalt").agg(
        F.count("*"),
        F.min("z"),
        F.max("z")
    ).count()

    return buckets, time.time() - t0

results = []
for t in CANDIDATE_TARGETS:
    b, sec = run_benchmark(t)
    results.append((t, b, sec))
    print(f"target={t:,} → buckets={b}, elapsed={sec:.2f}s")

feasible = [r for r in results if r[2] <= ACCEPTABLE_RUNTIME_SEC]
best = max(feasible, key=lambda r: r[0]) if feasible else min(results, key=lambda r: r[2])

TARGET_POINTS_PER_BUCKET, SALT_BUCKETS, _ = best

print("Final choice:",
      "targetPointsPerBucket =", TARGET_POINTS_PER_BUCKET,
      "saltBuckets =", SALT_BUCKETS)

# --------------------------------------------------
# 5) Write control table to Unity Catalog using saveAsTable
# --------------------------------------------------
print(f"\n=== Writing control parameters to Unity Catalog ===")

# Create DataFrame with control parameters
row = Row(
    siteId=SITE_ID,
    ingestRunId=INGEST_RUN_ID,
    tileSizeM=float(TILE_SIZE_M),
    hotTileThreshold=int(HOT_TILE_THRESHOLD),
    targetPointsPerBucket=int(TARGET_POINTS_PER_BUCKET),
    saltBuckets=int(SALT_BUCKETS),
    maxTilePoints=int(max_points),
    computedAt=datetime.now()
)

control_df = spark.createDataFrame([row])

# Write to Unity Catalog using saveAsTable
(
    control_df
    .write
    .format("delta")
    .mode("append")
    .option("path", CONTROL_PATH)  # Specify external location
    .saveAsTable(CONTROL_TABLE)
)

print(f"✅ Control parameters written and table registered in Unity Catalog")
print(f"   - Table: main.demo.{CONTROL_TABLE}")
print(f"   - Location: {CONTROL_PATH}")

# Verify table registration
print("\n=== Verify control table ===")
result = spark.sql(f"""
    SELECT siteId, ingestRunId, tileSizeM, hotTileThreshold, 
           targetPointsPerBucket, saltBuckets, maxTilePoints, computedAt
    FROM {CONTROL_TABLE}
    ORDER BY computedAt DESC
    LIMIT 5
""")
result.show(truncate=False)

print("\n✅ Complete!")
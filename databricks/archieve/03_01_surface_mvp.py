# 03_01_surface_mvp.py
from pyspark.sql import functions as F

# ====== Inputs (registered tables) ======
PROCESSED_TBL = "demo.demo_processed_points_tiled"
TILE_STATS_TBL = "demo.demo_agg_tile_stats"
WATER_MASK_TBL = "demo.demo_agg_tile_water_mask"  # optional

# ====== Outputs (paths) ======
SURFACE_CELLS_PATH = "abfss://aggregated@trimblegeospatialdemo.dfs.core.windows.net/surfaces/surface_cells"
SURFACE_PATCHES_PATH = "abfss://aggregated@trimblegeospatialdemo.dfs.core.windows.net/surfaces/surface_patches"

# ====== Params ======
SITE_ID = "wellington_cbd"
CELL_SIZE_M = 1.0
WATER_CLASS = 9

# Ground class guess: often ground=2 in LAS
GROUND_CLASSES = [2]   # adjust if needed
MAX_TILES_TO_PROCESS = None  # safety for demo (None to process all)

ALGO_VERSION = "mvp_grid_v1"

# ------------------------------------------------------------
# 1) Load tile candidates (from tile_stats, optionally join mostly-water)
# ------------------------------------------------------------
tile_stats = (
    spark.table(TILE_STATS_TBL)
    .filter(F.col("siteId") == SITE_ID)
)

# Discover ingestRunId automatically (assume single run for demo)
ingest_ids = [r["ingestRunId"] for r in tile_stats.select("ingestRunId").distinct().collect()]
if len(ingest_ids) != 1:
    raise ValueError(f"Expected 1 ingestRunId in tile_stats for site={SITE_ID}, found {ingest_ids}")
INGEST_RUN_ID = ingest_ids[0]
tile_stats = tile_stats.filter(F.col("ingestRunId") == INGEST_RUN_ID)

# Join water mask if available (adds isMostlyWater/waterRatioMask)
try:
    water_mask = (
        spark.table(WATER_MASK_TBL)
        .filter((F.col("siteId") == SITE_ID) & (F.col("ingestRunId") == INGEST_RUN_ID))
        # 🔧 FIX: Rename waterRatio to avoid ambiguity
        .select(
            "tileX",
            "tileY",
            "isMostlyWater",
            F.col("waterRatio").alias("waterRatioMask")  # Rename here!
        )
    )
    tile_candidates = (
        tile_stats.join(F.broadcast(water_mask), ["tileX","tileY"], "left")
        .withColumn("isMostlyWater", F.coalesce(F.col("isMostlyWater"), F.lit(0)))
        .withColumn("waterRatioMask", F.coalesce(F.col("waterRatioMask"), F.lit(0.0)))
    )
except Exception:
    tile_candidates = (
        tile_stats
        .withColumn("isMostlyWater", F.lit(0))
        .withColumn("waterRatioMask", F.lit(0.0))
    )

if MAX_TILES_TO_PROCESS is not None:
    tile_candidates = tile_candidates.orderBy(F.col("pointCount").desc()).limit(MAX_TILES_TO_PROCESS)

tile_candidates.cache()
print("Tiles to process:", tile_candidates.count())

# ------------------------------------------------------------
# 2) Load processed points and restrict to candidate tiles
# ------------------------------------------------------------
points = (
    spark.table(PROCESSED_TBL)
    .filter((F.col("siteId") == SITE_ID) & (F.col("ingestRunId") == INGEST_RUN_ID))
    .select("x","y","z","classification","tileX","tileY","siteId","ingestRunId")
)

tile_keys = tile_candidates.select("tileX","tileY").distinct()
points = points.join(F.broadcast(tile_keys), ["tileX","tileY"], "inner")

# ------------------------------------------------------------
# 3) Compute cell indices within each tile (stable origin per tile from tile_stats)
# ------------------------------------------------------------
tile_origins = tile_candidates.select("tileX","tileY","minX","minY","maxX","maxY","tileSizeM","isMostlyWater")

pts = (
    points.join(F.broadcast(tile_origins), ["tileX","tileY"], "inner")
    .withColumn("cellX", F.floor((F.col("x") - F.col("minX")) / F.lit(CELL_SIZE_M)).cast("int"))
    .withColumn("cellY", F.floor((F.col("y") - F.col("minY")) / F.lit(CELL_SIZE_M)).cast("int"))
)

# ------------------------------------------------------------
# 4) Build water + ground surfaces (cell-level)
# ------------------------------------------------------------
water_cells = (
    pts.filter(F.col("classification") == WATER_CLASS)
    .groupBy("siteId","ingestRunId","tileX","tileY","cellX","cellY")
    .agg(
        F.expr("percentile_approx(z, 0.5)").alias("z"),
        F.count("*").alias("pointCountCell")
    )
    .withColumn("surfaceType", F.lit("water"))
)

ground_cells = (
    pts.filter(F.col("classification").isin(GROUND_CLASSES))
    .groupBy("siteId","ingestRunId","tileX","tileY","cellX","cellY")
    .agg(
        F.expr("percentile_approx(z, 0.1)").alias("z"),
        F.count("*").alias("pointCountCell")
    )
    .withColumn("surfaceType", F.lit("ground"))
)

surface_cells = water_cells.unionByName(ground_cells)

# Add world bbox per cell (very useful for bbox serving)
surface_cells = (
    surface_cells.join(F.broadcast(tile_origins), ["tileX","tileY"], "inner")
    .withColumn("cellMinX", F.col("minX") + F.col("cellX") * F.lit(CELL_SIZE_M))
    .withColumn("cellMinY", F.col("minY") + F.col("cellY") * F.lit(CELL_SIZE_M))
    .withColumn("cellMaxX", F.col("cellMinX") + F.lit(CELL_SIZE_M))
    .withColumn("cellMaxY", F.col("cellMinY") + F.lit(CELL_SIZE_M))
    .select(
        "siteId","ingestRunId","surfaceType","tileX","tileY",
        "cellX","cellY","z","pointCountCell",
        "cellMinX","cellMinY","cellMaxX","cellMaxY",
        "tileSizeM"
    )
)

# ------------------------------------------------------------
# 5) Write surface_cells (Delta) - Dynamic Partition Overwrite
# ------------------------------------------------------------
print(f"Writing surface_cells for siteId={SITE_ID}, ingestRunId={INGEST_RUN_ID}...")

# 🔧 FIX: Use dynamic partition overwrite mode
# This will ONLY overwrite partitions being written (this site/run/surfaceType)
# Other partitions (other sites) remain untouched
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

(
    surface_cells
    .write.format("delta")
    .mode("overwrite")  # With dynamic mode, only overwrites matching partitions
    .partitionBy("siteId","ingestRunId","surfaceType")
    .save(SURFACE_CELLS_PATH)
)
print("✅ surface_cells written:", SURFACE_CELLS_PATH)
print(f"   Partitions overwritten: siteId={SITE_ID}, ingestRunId={INGEST_RUN_ID}, surfaceType=[water, ground]")

# ------------------------------------------------------------
# 6) Write surface_patches (tile-level index/summary)
# ------------------------------------------------------------
patches = (
    surface_cells
    .groupBy("siteId","ingestRunId","surfaceType","tileX","tileY")
    .agg(
        F.count("*").alias("cellCount"),
        F.sum("pointCountCell").alias("pointCountUsed"),
        F.min("cellMinX").alias("minX"),
        F.min("cellMinY").alias("minY"),
        F.max("cellMaxX").alias("maxX"),
        F.max("cellMaxY").alias("maxY"),
        F.avg("z").alias("avgZ")
    )
    .join(F.broadcast(tile_candidates.select("tileX","tileY","tileSizeM")), ["tileX","tileY"], "left")
    .withColumn("cellSizeM", F.lit(CELL_SIZE_M))
    .withColumn(
        "surfaceId",
        F.concat_ws("_", F.col("siteId"), F.col("ingestRunId"), F.col("tileX"), F.col("tileY"), F.col("surfaceType"))
    )
    # One URI to the cell store (cells are partitioned by site/run/type; API filters further by bbox)
    .withColumn("cellsUri", F.lit(SURFACE_CELLS_PATH))
    # rough coverage ratio estimate
    .withColumn(
        "quality",
        (F.col("cellCount") / (F.col("tileSizeM")*F.col("tileSizeM")/(F.lit(CELL_SIZE_M)*F.lit(CELL_SIZE_M))))
    )
    .withColumn("algoVersion", F.lit(ALGO_VERSION))
    .withColumn("computedAt", F.current_timestamp())
)

print(f"Writing surface_patches for siteId={SITE_ID}, ingestRunId={INGEST_RUN_ID}...")

(
    patches
    .write.format("delta")
    .mode("overwrite")  # Dynamic partition overwrite
    .partitionBy("siteId","ingestRunId","surfaceType")
    .save(SURFACE_PATCHES_PATH)
)
print("✅ surface_patches written:", SURFACE_PATCHES_PATH)
print(f"   Partitions overwritten: siteId={SITE_ID}, ingestRunId={INGEST_RUN_ID}, surfaceType=[water, ground]")

print("\n=== Top 20 patches by point density ===")
patches.orderBy(F.col("pointCountUsed").desc()).show(20, truncate=False)

print("\n✅ Surface generation complete!")
print(f"   Safe to re-run: Only {SITE_ID}/{INGEST_RUN_ID} partitions will be overwritten")
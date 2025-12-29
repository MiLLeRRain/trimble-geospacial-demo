# 01_ingest_raw
from pyspark.sql import functions as F

landing_path = "abfss://raw-landing@trimblegeospatialdemo.dfs.core.windows.net/staging/points_ingest.parquet"
raw_path     = "abfss://raw@trimblegeospatialdemo.dfs.core.windows.net/points"

# 1) Read from raw-landing (staging)
df = spark.read.parquet(landing_path)

# 2) Minimal safety checks (optional but recommended)
required_cols = ["x","y","z","intensity","classification","return_number","number_of_returns","gps_time","siteId","ingestRunId","ingestTime"]
missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"Missing required columns: {missing}")

# Ensure ingestTime is timestamp (sometimes it comes in as string)
df = df.withColumn("ingestTime", F.col("ingestTime").cast("timestamp"))

# Optional: basic sanity filter (keep it light; still considered 'raw safe' if you only drop null geometry)
df = df.filter(F.col("x").isNotNull() & F.col("y").isNotNull() & F.col("z").isNotNull())

# 3) Write to Raw Delta (recommended)
(
    df.write
      .format("delta")
      .mode("append")                  # append new runs safely
      .partitionBy("siteId","ingestRunId")
      .save(raw_path)
)

print("✅ Write complete. Verifying...")

# 4) Verify write
written = spark.read.format("delta").load(raw_path)

# Verify this run landed (lightweight check)
df.select("siteId","ingestRunId").distinct().show(truncate=False)

written.filter(
    (F.col("siteId") == df.select("siteId").first()[0]) &
    (F.col("ingestRunId") == df.select("ingestRunId").first()[0])
).count()

print(f"✅ Raw Delta table now contains {written.count():,} rows total.")

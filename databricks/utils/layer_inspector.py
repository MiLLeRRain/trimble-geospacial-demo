# utils/layer_inspector.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Dict, Any, List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class LakehousePaths:
    """
    Central place to keep all layer paths.
    Edit these once, reuse everywhere.
    """
    raw_points: str = "abfss://raw@trimblegeospatialdemo.dfs.core.windows.net/points"
    processed_points: str = "abfss://processed@trimblegeospatialdemo.dfs.core.windows.net/points_tiled"
    aggregated_tile_stats: str = "abfss://aggregated@trimblegeospatialdemo.dfs.core.windows.net/tile_stats"
    aggregated_tile_water_mask: str = "abfss://aggregated@trimblegeospatialdemo.dfs.core.windows.net/tile_water_mask"
    control_tiling_params: str = "abfss://aggregated@trimblegeospatialdemo.dfs.core.windows.net/control/tiling_params"


def _try_read_delta(spark, path: str) -> Optional[DataFrame]:
    try:
        return spark.read.format("delta").load(path)
    except Exception as e:
        print(f"❌ Failed to read Delta at: {path}")
        print(f"   Reason: {str(e)[:200]}")
        return None


def _filter_site_run(df: DataFrame, site_id: str, ingest_run_id: str) -> DataFrame:
    cols = set(df.columns)
    if "siteId" in cols and "ingestRunId" in cols:
        return df.filter((F.col("siteId") == site_id) & (F.col("ingestRunId") == ingest_run_id))
    if "siteId" in cols:
        return df.filter(F.col("siteId") == site_id)
    if "ingestRunId" in cols:
        return df.filter(F.col("ingestRunId") == ingest_run_id)
    # If table doesn't have either column, return as-is
    return df


def _quick_metrics(df: DataFrame, label: str) -> None:
    cols = set(df.columns)
    agg_exprs = [F.count("*").alias("rows")]
    if "tileX" in cols and "tileY" in cols:
        agg_exprs += [F.countDistinct("tileX", "tileY").alias("distinctTiles")]
    if "classification" in cols:
        # keep light: just count distinct classes
        agg_exprs += [F.countDistinct("classification").alias("distinctClassifications")]
    if "ingestTime" in cols:
        agg_exprs += [F.min("ingestTime").alias("minIngestTime"), F.max("ingestTime").alias("maxIngestTime")]

    print(f"\n--- {label}: metrics ---")
    df.agg(*agg_exprs).show(truncate=False)


def _show_sample(df: DataFrame, label: str, n: int) -> None:
    print(f"\n--- {label}: sample top {n} ---")
    df.show(n, truncate=False)


def inspect_layers(
    spark,
    *,
    site_id: str,
    ingest_run_id: str,
    paths: LakehousePaths = LakehousePaths(),
    sample_n: int = 20,
    show_schema: bool = False,
    layers: list[str] | None = None,   # 👈 add this
) -> None:
    """
    Inspect Raw/Processed/Aggregated/Control quickly for a given site+run.
    Prints:
      - whether each layer exists
      - metrics
      - sample rows
      - optional schema
    """

    layer_map = {
        "control": ("CONTROL.tiling_params", paths.control_tiling_params),
        "raw": ("RAW.points", paths.raw_points),
        "processed": ("PROCESSED.points_tiled", paths.processed_points),
        "aggregated": ("AGG.tile_stats", paths.aggregated_tile_stats),
        "aggregated_water": ("AGG.tile_water_mask", paths.aggregated_tile_water_mask),
    }

    # if layers is None -> show all
    selected = layers or ["control", "raw", "processed", "aggregated", "aggregated_water"]

    print("\n==============================================")
    print(f"🔎 Inspecting layers for siteId={site_id}, ingestRunId={ingest_run_id}")
    print("==============================================")

    for key in selected:
        label, path = layer_map[key]
        print(f"\n✅ Layer: {label}")
        print(f"Path: {path}")

        df = _try_read_delta(spark, path)
        if df is None:
            continue

        df_scoped = _filter_site_run(df, site_id, ingest_run_id)

        if show_schema:
            print(f"\n--- {label}: schema ---")
            df_scoped.printSchema()

        # If scoped dataset empty, say so and show a hint (top 5 of unfiltered)
        if df_scoped.limit(1).count() == 0:
            print(f"⚠️ {label}: No rows found for siteId/runId filter.")
            print("   Showing top 5 unfiltered rows (for debugging):")
            df.limit(5).show(truncate=False)
            continue

        _quick_metrics(df_scoped, label)
        _show_sample(df_scoped, label, sample_n)

        # Extra useful: classification distribution for point tables
        cols = set(df_scoped.columns)
        if "classification" in cols:
            print(f"\n--- {label}: classification distribution (top 20) ---")
            (df_scoped.groupBy("classification").count()
                     .orderBy(F.col("count").desc())
                     .show(20, truncate=False))

    print("\n✅ Done.\n")


def discover_single_ingest_run_id(spark, *, raw_path: str, site_id: str) -> str:
    """
    Convenience helper: find the single ingestRunId for a site in RAW.
    Raises if 0 or >1.
    """
    df = spark.read.format("delta").load(raw_path).filter(F.col("siteId") == site_id)
    ids = [r["ingestRunId"] for r in df.select("ingestRunId").distinct().collect()]
    if len(ids) != 1:
        raise ValueError(f"Expected exactly one ingestRunId for site={site_id}, found: {ids}")
    return ids[0]

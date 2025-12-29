from layer_inspector import inspect_layers, discover_single_ingest_run_id, LakehousePaths

site_id = "wellington_cbd"
paths = LakehousePaths()

ingest_run_id = discover_single_ingest_run_id(spark, raw_path=paths.raw_points, site_id=site_id)

inspect_layers(
    spark,
    site_id=site_id,
    ingest_run_id=ingest_run_id,
    layers=["aggregated", "aggregated_water"],
    sample_n=20
)

# path options:
# ["control", "raw", "processed", "aggregated", "aggregated_water"]
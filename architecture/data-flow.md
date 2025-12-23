# Data Flow

Upload -> Process -> Serve

Notes
- Raw data lands in `abfss://raw@<storage>.dfs.core.windows.net/`
- Processing writes Delta tables to `processed`
- Aggregations for API reads live in `aggregated`

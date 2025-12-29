-- 04_optimize_delta.sql
-- Configure Liquid Clustering for all Delta tables
-- 
-- Key Principles:
-- 1. Max 4 clustering keys (Databricks limit)
-- 2. Don't duplicate partition keys in clustering keys (redundant)
-- 3. Choose high-cardinality columns frequently used in filters
-- 4. Focus on spatial columns (tileX, tileY) for optimal query performance
USE CATALOG main;
USE SCHEMA demo;

-- ============================================================
-- Processed Layer
-- ============================================================

-- Processed points (tiled point cloud)
-- Partition: siteId, ingestRunId
-- Clustering: tileX, tileY (spatial queries)
ALTER TABLE demo_processed_points_tiled
CLUSTER BY (tileX, tileY);

-- ============================================================
-- Aggregated Layer
-- ============================================================

-- Tile statistics (tile-level summary)
-- Partition: siteId, ingestRunId
-- Clustering: tileX, tileY (spatial queries)
ALTER TABLE demo_agg_tile_stats
CLUSTER BY (tileX, tileY);

-- Tile water mask (water tiles only)
-- Partition: siteId, ingestRunId
-- Clustering: tileX, tileY (spatial queries)
ALTER TABLE demo_agg_tile_water_mask
CLUSTER BY (tileX, tileY);

-- Surface patches (tile-level surface metadata)
-- Partition: siteId, ingestRunId, surfaceType
-- Clustering: tileX, tileY (spatial queries, partition keys excluded)
ALTER TABLE demo_agg_surface_patches
CLUSTER BY (tileX, tileY);

-- Surface cells (cell-level surface data)
-- Partition: siteId, ingestRunId, surfaceType
-- Clustering: tileX, tileY (spatial queries, partition keys excluded)
ALTER TABLE demo_agg_surface_cells
CLUSTER BY (tileX, tileY);

-- ============================================================
-- Verify Clustering Configuration
-- ============================================================

SELECT 
  'demo_processed_points_tiled' AS table_name,
  'tileX, tileY' AS clustering_keys,
  'siteId, ingestRunId' AS partition_keys
UNION ALL
SELECT 
  'demo_agg_tile_stats',
  'tileX, tileY',
  'siteId, ingestRunId'
UNION ALL
SELECT 
  'demo_agg_tile_water_mask',
  'tileX, tileY',
  'siteId, ingestRunId'
UNION ALL
SELECT 
  'demo_agg_surface_patches',
  'tileX, tileY',
  'siteId, ingestRunId, surfaceType'
UNION ALL
SELECT 
  'demo_agg_surface_cells',
  'tileX, tileY',
  'siteId, ingestRunId, surfaceType';

-- ============================================================
-- Optional: Run OPTIMIZE to apply clustering
-- ============================================================
-- Uncomment and run these after configuring clustering:

-- OPTIMIZE demo.demo_processed_points_tiled;
-- OPTIMIZE demo.demo_agg_tile_stats;
-- OPTIMIZE demo.demo_agg_tile_water_mask;
-- OPTIMIZE demo.demo_agg_surface_patches;
-- OPTIMIZE demo.demo_agg_surface_cells;
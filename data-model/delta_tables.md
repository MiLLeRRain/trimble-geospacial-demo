%md
# Delta Tables (v2)

## Processed Data

### processed_points_tiled_v2
**Purpose**: Processed point cloud data organized in tiled format for efficient spatial queries

- **Key Columns**: `x`, `y`, `z`, `intensity`, `classification`, `return_number`, `number_of_returns`, `gps_time`, `tileX`, `tileY`, `tileSalt`, `isHotTile`
- **Partition**: `siteId`, `tileId`
- **Use Cases**: 3D modeling, terrain analysis, environmental monitoring
- **Note**: `tileId` format is `tileX_tileY`

---

## Aggregated Statistics

### tile_stats_v2
**Purpose**: Tile-level statistics for quick spatial analysis and filtering

- **Key Columns**: `pointCount`, `minZ`, `maxZ`, `z_p50`, `z_p95`, `z_p99`, `intensity_p50`, `intensity_p95`, `waterPointCount`, `waterPointRatio`, `isMostlyWater`, `computedAt`
- **Partition**: `siteId`, `tileId`
- **Use Cases**: Tile filtering, water detection, elevation analysis
- **Z-Order**: `isMostlyWater`, `waterPointRatio`

---

## Surface Analysis

### surface_cells_v2
**Purpose**: Fine-grained surface cells with elevation and water statistics

- **Key Columns**: `cellX`, `cellY`, `pointCount`, `waterPointCount`, `minZ`, `meanZ`, `maxZ`, `z_p50`, `waterPointRatio`, `cellSizeM`, `computedAt`
- **Partition**: `siteId`, `tileId`
- **Use Cases**: Surface modeling, water body detection, terrain analysis
- **Z-Order**: `cellX`, `cellY`

### surface_patches_v2
**Purpose**: Coarse-grained surface patches for efficient spatial grouping

- **Key Columns**: `patchId`, `cellCount`, `pointsUsed`, `waterPointCount`, `minZ`, `maxZ`, `meanZ`, `waterPointRatio`, `surfaceType`, `computedAt`
- **Partition**: `siteId`, `tileId`
- **Use Cases**: Surface type classification, spatial aggregation
- **Z-Order**: `patchId`, `surfaceType`
- **Note**: `patchId` format is `patchX_patchY`

---

## Feature Extraction

### features_water_bodies_v2
**Purpose**: Identified water bodies with area and bounding box information

- **Key Columns**: `waterBodyId`, `cellCount`, `areaM2`, `minZ`, `maxZ`, `meanZ`, `bboxMinCellX`, `bboxMinCellY`, `bboxMaxCellX`, `bboxMaxCellY`, `computedAt`
- **Partition**: `siteId`
- **Use Cases**: Water resource mapping, environmental analysis, land use planning
- **Z-Order**: `waterBodyId`, `areaM2`
- **Note**: Uses GraphFrames connected components for water body identification

### features_building_candidates_v2
**Purpose**: Potential building locations with height and spatial metrics

- **Key Columns**: `buildingCandidateId`, `pointsUsed`, `cellCount`, `minZ`, `meanZ`, `maxZ`, `heightAboveGround`, `heightRange`, `computedAt`
- **Partition**: `siteId`, `tileId`
- **Use Cases**: Building detection, urban planning, infrastructure analysis
- **Z-Order**: `buildingCandidateId`, `heightAboveGround`

---

## Common Patterns

- **Spatial Indexing**: All tables use `tileId` (format: `tileX_tileY`) for spatial partitioning
- **Temporal Tracking**: All tables include `computedAt` timestamp for versioning
- **Site Isolation**: All tables partitioned by `siteId` for multi-tenant support
- **Z-Ordering**: Applied on frequently queried data columns (not partition columns)
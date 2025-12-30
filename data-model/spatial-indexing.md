%md
# Spatial Indexing

## Strategy
- Discretize coordinates to tiles
- Use `tileId` (format: `tileX_tileY`) as spatial key
- Query by bbox → compute tile range → filter by `tileId`

## Implementation
- `tileId` = `"{tileX}_{tileY}"` (e.g., `"123_456"`)
- Partition tables by `siteId` and `tileId`
- Parse `tileX` and `tileY` from `tileId` when needed for spatial queries

## Notes
- Use tile size consistent across pipeline (default: 25m)
- Avoid excessive partitions (balance between granularity and overhead)
- `tileId` enables efficient spatial filtering and join operations
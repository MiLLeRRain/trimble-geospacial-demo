%md
# Delta Tables

## tiles (processed)
- **Columns**: `siteId`, `tileX`, `tileY`, `pointsCount`, `avgZ`, `minZ`, `maxZ`
- **Partition**: `siteId`
- **Liquid clustering**: `tileX`, `tileY`

## tile_stats (aggregated)
- **Columns**: `siteId`, `ingestRunId`, `tileX`, `tileY`, `pointCount`, `waterCount`, `minX`, `maxX`, `minY`, `maxY`, `minZ`, `maxZ`, `avgZ`, `waterRatio`, `tileSizeM`
- **Partition**: `siteId`
- **Liquid clustering**: `tileX`, `tileY`

## tile_water_mask (aggregated)
- **Columns**: `siteId`, `ingestRunId`, `tileX`, `tileY`, `waterRatio`, `minX`, `minY`, `maxX`, `maxY`, `tileSizeM`, `isMostlyWater`
- **Partition**: `siteId`
- **Liquid clustering**: `tileX`, `tileY`
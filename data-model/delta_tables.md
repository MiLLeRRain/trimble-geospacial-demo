# Delta Tables

## tiles (processed)
- Columns: siteId, tileX, tileY, pointsCount, avgZ, minZ, maxZ
- Partition: siteId
- Liquid clustering: tileX, tileY

## tile_stats (aggregated)
- Columns: siteId, tileX, tileY, minX, maxX, minY, maxY, avgZ
- Partition: siteId
- Liquid clustering: tileX, tileY

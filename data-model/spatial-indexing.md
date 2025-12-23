# Spatial Indexing

Strategy
- Discretize coordinates to tiles
- Use (tileX, tileY) as spatial keys
- Query by bbox -> tile range

Notes
- Use tile size consistent across pipeline
- Avoid excessive partitions

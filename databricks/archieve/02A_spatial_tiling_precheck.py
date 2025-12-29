# 02_spatial_tiling - Path exploration and correction
from pyspark.sql import functions as F

# The issue: Your path had '/raw/points/' but the container is already 'raw'
# So the path should be just '/points/' (without the /raw prefix)

print("=== Step 1: Explore what's in the storage ===\n")

# Check what's in the root of 'raw' container
print("Listing root of 'raw' container:")
try:
    root_files = dbutils.fs.ls("abfss://raw@trimblegeospatialdemo.dfs.core.windows.net/")
    for f in root_files:
        print(f"  - {f.name} ({'dir' if f.isDir() else 'file'})")
except Exception as e:
    print(f"  Error: {str(e)[:150]}")

print("\n" + "="*60)
print("=== Step 2: Check points directory ===\n")

# Check what's in points directory
try:
    points_files = dbutils.fs.ls("abfss://raw@trimblegeospatialdemo.dfs.core.windows.net/points/")
    print(f"Found {len(points_files)} items in points/:")
    for f in points_files[:10]:
        print(f"  - {f.name} ({'dir' if f.isDir() else 'file'})")
    if len(points_files) > 10:
        print(f"  ... and {len(points_files) - 10} more items")
except Exception as e:
    print(f"  Error: {str(e)[:150]}")

print("\n" + "="*60)
print("=== Step 3: Try reading the Delta table ===\n")

# Corrected path (removed /raw prefix since container is already 'raw')
correct_path = "abfss://raw@trimblegeospatialdemo.dfs.core.windows.net/points/siteId=wellington_cbd/ingestRunId=20130101"

print(f"Attempting to read from:\n{correct_path}\n")

try:
    df = spark.read.format("delta").load(correct_path)
    
    print("✅ Successfully loaded Delta table!\n")
    print(f"Schema: {', '.join(df.columns)}")
    print(f"\nTotal records: {df.count():,}\n")
    
    # Get distinct values of classification column with counts
    print("Distinct classification values:")
    df.groupBy("classification").count().orderBy("count", ascending=False).show()
    
except Exception as e:
    print(f"❌ Error: {str(e)}\n")
    print("\nTroubleshooting tips:")
    print("1. Check if the path exists using dbutils.fs.ls()")
    print("2. Verify the partition values (siteId and ingestRunId)")
    print("3. Check if it's actually a Delta table (_delta_log directory exists)")
    print("4. Try reading without partition filters first")
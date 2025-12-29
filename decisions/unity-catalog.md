%md
# Unity Catalog Migration Guide - Problems & Solutions

## 📋 Table of Contents
1. [Background & Problem](#background)
2. [Root Cause Analysis](#root-cause)
3. [Solution Overview](#solution)
4. [Step-by-Step Implementation](#implementation)
5. [Common Issues & Fixes](#issues)
6. [Best Practices](#best-practices)
7. [Final Configuration](#final-config)

---

## 🎯 Background & Problem {#background}

### Initial Problem
**Symptom**: Every time the Databricks cluster restarts, all Delta Table schemas and table registrations in `hive_metastore` are lost.

**Impact**:
* Need to re-run `CREATE TABLE` statements after every cluster restart
* Table metadata is not persistent
* Cannot reliably access tables via API or JDBC/ODBC
* Data files in ADLS are intact, but table registrations disappear

**Environment**:
* Azure Databricks workspace
* Delta Tables stored in ADLS Gen2 (`trimblegeospatialdemo` storage account)
* Cluster: `demo-job-cluster` (initially `LEGACY_SINGLE_USER_STANDARD` mode)
* 5 Delta Tables with 50+ million rows

---

## 🔍 Root Cause Analysis {#root-cause}

### Why Tables Were Lost

1. **Using temporary `hive_metastore`**
   * Default Databricks metastore is **temporary** and tied to cluster lifecycle
   * Metadata stored in cluster's local storage
   * When cluster terminates, metastore data is deleted

2. **No persistent metastore configured**
   * No external Hive metastore (e.g., Azure SQL Database)
   * No Unity Catalog enabled
   * Table registrations only exist in memory

3. **Cluster access mode limitation**
   * `LEGACY_SINGLE_USER_STANDARD` mode does not support Unity Catalog
   * Cannot use modern Unity Catalog features

---

## ✅ Solution Overview {#solution}

### Chosen Solution: Unity Catalog

**Why Unity Catalog?**
* ✅ **Persistent metadata**: Survives cluster restarts
* ✅ **Unified governance**: Centralized access control
* ✅ **API-friendly**: REST API, JDBC/ODBC, SDK support
* ✅ **Future-proof**: Databricks' recommended approach
* ✅ **Delta Sharing**: Can share data across workspaces/organizations

**Alternative (not chosen)**: External Hive Metastore
* Requires Azure SQL Database setup and maintenance
* Legacy approach, being phased out
* Less feature-rich than Unity Catalog

---

## 🛠️ Step-by-Step Implementation {#implementation}

### Step 1: Create Access Connector for Azure Databricks

**Purpose**: Allows Unity Catalog to access ADLS using Managed Identity

**Steps**:
1. In Azure Portal, create **Access Connector for Azure Databricks**
2. Resource Group: `trimble-geospatial-demo-rg`
3. Name: `databricks-access-connector`
4. Region: Same as storage account (`australiaeast`)
5. Enable **System-assigned managed identity**
6. Copy Resource ID:
   ```
   /subscriptions/c3f5be8c-0562-4797-90ef-d4bde0c2fffa/resourceGroups/trimble-geospatial-demo-rg/providers/Microsoft.Databricks/accessConnectors/databricks-access-connector
   ```

### Step 2: Create Unity Catalog Metastore

**Purpose**: Central metadata repository for all catalogs, schemas, and tables

**Steps**:
1. Login to [Databricks Account Console](https://accounts.azuredatabricks.net)
2. Navigate to **Data** → **Unity Catalog**
3. Click **Create Metastore**
4. Configuration:
   * **Name**: `main`
   * **Region**: `australiaeast`
   * **ADLS Gen2 path**: `abfss://unity-metastore@trimblegeospatialdemo.dfs.core.windows.net/b2ee7d21-1e8e-4303-85d1-85ab38872054`
   * **Access Connector ID**: (paste from Step 1)
5. Click **Create**

### Step 3: Assign Metastore to Workspace

**Steps**:
1. In Account Console, go to **Workspaces**
2. Select workspace: `trimble-geospatial-demo-ws`
3. Click **Assign Metastore**
4. Select `main` metastore
5. Confirm assignment

### Step 4: Grant Metastore Admin Permissions

**Steps**:
1. In Account Console, go to **Data** → **Unity Catalog** → `main` metastore
2. Click **Permissions**
3. Add user as **Metastore Admin**
4. Save

### Step 5: Create Storage Credential

**Purpose**: Authenticate Unity Catalog to access ADLS containers

**Steps** (via Databricks UI):
1. In Databricks Workspace, click **Catalog** (left sidebar)
2. Go to **External Data** → **Storage Credentials**
3. Click **Create Credential**
4. Configuration:
   * **Name**: `trimble_adls_credential`
   * **Type**: Azure Managed Identity
   * **Access Connector ID**: (paste from Step 1)
5. Click **Create**

### Step 6: Create External Locations

**Purpose**: Register ADLS containers as accessible locations for Unity Catalog

**For `processed` container**:
1. Go to **External Data** → **External Locations**
2. Click **Create Location**
3. Configuration:
   * **Name**: `processed_container`
   * **URL**: `abfss://processed@trimblegeospatialdemo.dfs.core.windows.net/`
   * **Storage Credential**: `trimble_adls_credential`
4. Click **Create**

**For `aggregated` container**:
1. Repeat above steps
2. Configuration:
   * **Name**: `aggregated_container`
   * **URL**: `abfss://aggregated@trimblegeospatialdemo.dfs.core.windows.net/`
   * **Storage Credential**: `trimble_adls_credential`
3. Click **Create**

### Step 7: Update Cluster Access Mode

**Purpose**: Enable Unity Catalog support on existing cluster

**Steps**:
1. In Databricks Workspace, go to **Compute**
2. Select cluster: `demo-job-cluster`
3. Click **Edit**
4. In **Advanced Options** → **Access Mode**:
   * Change from `LEGACY_SINGLE_USER_STANDARD` to **Single User**
5. Keep existing Spark configurations (ADLS OAuth settings)
6. Save and restart cluster

### Step 8: Register Tables in Unity Catalog

**Purpose**: Register existing Delta Tables from ADLS into Unity Catalog

**SQL Code**:
```sql
USE CATALOG main;

CREATE SCHEMA IF NOT EXISTS demo
COMMENT 'Demo schema for geospatial data';

USE SCHEMA demo;

CREATE TABLE IF NOT EXISTS demo_processed_points_tiled
USING DELTA
LOCATION 'abfss://processed@trimblegeospatialdemo.dfs.core.windows.net/points_tiled';

CREATE TABLE IF NOT EXISTS demo_agg_tile_stats
USING DELTA
LOCATION 'abfss://aggregated@trimblegeospatialdemo.dfs.core.windows.net/tile_stats';

-- ... (other tables)
```

---

## ⚠️ Common Issues & Fixes {#issues}

### Issue 1: `UC_HIVE_METASTORE_DISABLED_EXCEPTION`

**Error**: `The operation attempted to use Hive Metastore for schema hive_metastore.default, which is disabled...`

**Solution**: In SQL Editor, manually switch **Catalog dropdown** from `hive_metastore` to `main`

### Issue 2: `EXTERNAL_LOCATION_DOES_NOT_EXIST`

**Error**: `External Location 'abfss://...' does not exist.`

**Solution**: Remove `MANAGED LOCATION` from `CREATE SCHEMA` or create External Location first

### Issue 3: `NO_PARENT_EXTERNAL_LOCATION_FOR_PATH`

**Error**: `No parent external location was found for path 'abfss://processed@...'`

**Solution**: Create External Locations for all ADLS containers (see Step 6)

### Issue 4: `PERMISSION_DENIED: User does not have CREATE SCHEMA`

**Solution**: Grant permissions:
```sql
GRANT CREATE SCHEMA ON CATALOG main TO `user@example.com`;
```

### Issue 5: Cannot use `ALTER TABLE CLUSTER BY` on partitioned tables

**Solution**: Use `OPTIMIZE ... ZORDER BY` instead:
```sql
OPTIMIZE demo_processed_points_tiled ZORDER BY (tileX, tileY);
```

---

## 📚 Best Practices {#best-practices}

### 1. Always Specify Catalog and Schema

```sql
USE CATALOG main;
USE SCHEMA demo;
```

Or use three-part naming:
```sql
SELECT * FROM main.demo.demo_processed_points_tiled;
```

### 2. Avoid Two-Part Naming

```sql
-- ❌ Wrong (Unity Catalog interprets 'demo' as catalog name)
SELECT * FROM demo.demo_processed_points_tiled;

-- ✅ Correct
SELECT * FROM demo_processed_points_tiled;
```

### 3. External vs Managed Tables

**External Tables** (recommended for existing data):
```sql
CREATE TABLE my_table USING DELTA LOCATION 'abfss://...';
```

**Managed Tables** (for new data):
```sql
CREATE TABLE my_table (id INT, name STRING);
```

### 4. Optimization Strategy

**For partitioned tables**: Use `OPTIMIZE ... ZORDER BY`
```sql
OPTIMIZE demo_processed_points_tiled ZORDER BY (tileX, tileY);
```

---

## 🎯 Final Configuration {#final-config}

### Unity Catalog Hierarchy
```
Metastore: main
└── Catalog: main
    └── Schema: demo
        ├── demo_processed_points_tiled (45M rows)
        ├── demo_agg_tile_stats (20K rows)
        ├── demo_agg_tile_water_mask (5K rows)
        ├── demo_agg_surface_cells (5.7M rows)
        ├── demo_agg_surface_patches (20K rows)
        └── demo_features_building_candidates (new)
```

### Storage Configuration
* **Metastore Root**: `abfss://unity-metastore@trimblegeospatialdemo.dfs.core.windows.net/...`
* **Storage Credential**: `trimble_adls_credential` (Azure Managed Identity)
* **External Locations**: `processed_container`, `aggregated_container`

### Cluster Configuration
* **Name**: `demo-job-cluster`
* **Access Mode**: Single User (Unity Catalog enabled)
* **Runtime**: DBR 16.4 LTS

---

## ✅ Verification Checklist

* [x] Unity Catalog metastore created and assigned to workspace
* [x] Access Connector created with managed identity
* [x] Storage Credential created
* [x] External Locations registered
* [x] Cluster access mode changed to Single User
* [x] All tables registered in Unity Catalog
* [x] Tables accessible after cluster restart
* [x] ZORDER optimization configured

---

**Document Created**: 2025-12-26  
**Author**: Apang Jin  
**Workspace**: trimble-geospatial-demo-ws
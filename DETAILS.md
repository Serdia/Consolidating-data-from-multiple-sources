# Technical Deep-Dive — Incremental Data Integration Pipeline

## Table of Contents
1. [Problem Statement](#1-problem-statement)
2. [Change Tracking Setup](#2-change-tracking-setup)
3. [Metadata Control Table](#3-metadata-control-table)
4. [ADF Pipeline Walkthrough](#4-adf-pipeline-walkthrough)
5. [Staging Layer](#5-staging-layer)
6. [Dynamic MERGE Stored Procedure](#6-dynamic-merge-stored-procedure)
7. [Audit & Watermark Management](#7-audit--watermark-management)
8. [Parquet Source Handling](#8-parquet-source-handling)
9. [Error Handling & Retry Logic](#9-error-handling--retry-logic)
10. [Design Decisions](#10-design-decisions)

---

## 1. Problem Statement

Four operational on-premises systems generate transactional data continuously:
- **3 SQL Server databases** — policy, claims, and CRM data
- **1 Azure Blob Storage container** — vendor-delivered Parquet files

The business requirement was to consolidate this data into a single Azure SQL Database for reporting and downstream analytics, with a maximum latency of **15 minutes** and no impact on source system performance.

Full table loads were not viable — source tables range from 500K to 5M+ rows and are actively used by production applications during business hours.

---

## 2. Change Tracking Setup

SQL Server Change Tracking (CT) was chosen over CDC (Change Data Capture) because:
- No dependency on SQL Server Agent or log reader processes
- Lower overhead on source systems
- Sufficient for 15-minute latency (no need for sub-second replication)
- Simpler operational maintenance

### Enabling Change Tracking on Source

```sql
-- Enable at database level
ALTER DATABASE [SourceDB]
SET CHANGE_TRACKING = ON
(CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);

-- Enable at table level
ALTER TABLE [IMSA].[tblQuotes]
ENABLE CHANGE_TRACKING
WITH (TRACK_COLUMNS_UPDATED = ON);
```

### Querying the Delta

```sql
-- Get all rows changed since last known version
SELECT 
    ct.QuoteID,
    ct.SYS_CHANGE_OPERATION,   -- 'I' = Insert, 'U' = Update, 'D' = Delete
    ct.SYS_CHANGE_VERSION,
    q.*
FROM CHANGETABLE(CHANGES [IMSA].[tblQuotes], @last_sync_version) AS ct
LEFT JOIN [IMSA].[tblQuotes] q ON ct.QuoteID = q.QuoteID
```

`@last_sync_version` is retrieved from the watermark control table at the start of each pipeline run.

---

## 3. Metadata Control Table

A control table in Azure SQL drives the entire pipeline dynamically. No hardcoded table names in ADF.

```sql
CREATE TABLE [control].[SourceTableConfig] (
    TableConfigID       INT IDENTITY PRIMARY KEY,
    SourceServer        NVARCHAR(200),
    SourceDatabase      NVARCHAR(100),
    SourceSchema        NVARCHAR(100),
    SourceTable         NVARCHAR(100),
    DestSchema          NVARCHAR(100),
    DestTable           NVARCHAR(100),
    PrimaryKeyColumn    NVARCHAR(200),
    IsActive            BIT DEFAULT 1,
    LastSyncVersion     BIGINT DEFAULT 0,      -- Change Tracking watermark
    LastLoadStart       DATETIME2,
    LastLoadEnd         DATETIME2,
    LastLoadStatus      NVARCHAR(50),          -- 'Running', 'Success', 'Failed'
    LastRowsLoaded      INT
);
```

Adding a new source table to the pipeline = inserting one row into this table.

---

## 4. ADF Pipeline Walkthrough

### Activity 1 — GetTableMetaData (Lookup)
Queries `control.SourceTableConfig` where `IsActive = 1`. Returns all tables to be processed. Output feeds a **ForEach** activity that parallelizes per-table loads.

### Activity 2 — StartSourceTableLoad (Stored Procedure)
Called at the start of each table iteration. Does two things:
- Updates `LastLoadStart`, sets `LastLoadStatus = 'Running'`
- Returns `@last_sync_version` to ADF as an output parameter — used in the Copy Data query

```sql
CREATE PROCEDURE [control].[StartSourceTableLoad]
    @TableConfigID INT,
    @LastSyncVersion BIGINT OUTPUT
AS
BEGIN
    UPDATE [control].[SourceTableConfig]
    SET LastLoadStart = GETUTCDATE(),
        LastLoadStatus = 'Running'
    WHERE TableConfigID = @TableConfigID;

    SELECT @LastSyncVersion = LastSyncVersion
    FROM [control].[SourceTableConfig]
    WHERE TableConfigID = @TableConfigID;
END
```

### Activity 3 — Copy Table (Copy Data)
- **Source:** Parameterized SQL query using `CHANGETABLE()` with `@last_sync_version` from Step 2
- **Destination:** Staging table in Azure SQL (`staging.tblQuotes_stg`)
- **Write behavior:** Truncate staging table before each load (staging is always a clean snapshot of the current delta)

### Activity 4 — Merge Staged Data (Stored Procedure)
Merges staging into persistent table. See Section 6 for details.

### Activity 5 — CompleteSourceTableLoad / CompleteSourceTableLoad_Failure
See Section 7.

---

## 5. Staging Layer

Each source table has a corresponding staging table in Azure SQL. Staging tables mirror the source schema plus metadata columns:

```sql
-- Example staging table
CREATE TABLE [staging].[tblQuotes_stg] (
    -- All source columns
    QuoteID             INT,
    QuoteGUID           UNIQUEIDENTIFIER,
    EffectiveDate       DATE,
    -- ... other columns ...

    -- Change Tracking metadata
    CT_Operation        CHAR(1),        -- 'I', 'U', 'D'
    CT_Version          BIGINT,
    _LoadedAt           DATETIME2 DEFAULT GETUTCDATE()
);
```

Staging is **truncated before each load** — it holds only the current delta batch, not historical data.

---

## 6. Dynamic MERGE Stored Procedure

Rather than writing a separate MERGE statement for each of the dozens of tables, a single stored procedure generates and executes the MERGE dynamically based on `INFORMATION_SCHEMA` metadata.

```sql
CREATE PROCEDURE [control].[MergeStagedData]
    @DestSchema     NVARCHAR(100),
    @DestTable      NVARCHAR(100),
    @StagingSchema  NVARCHAR(100),
    @StagingTable   NVARCHAR(100),
    @PKColumn       NVARCHAR(200)
AS
BEGIN
    DECLARE @merge_sql NVARCHAR(MAX);
    DECLARE @col_list  NVARCHAR(MAX) = '';
    DECLARE @upd_list  NVARCHAR(MAX) = '';

    -- Build column lists dynamically from schema
    SELECT 
        @col_list += '[' + COLUMN_NAME + '], ',
        @upd_list += 'target.[' + COLUMN_NAME + '] = source.[' + COLUMN_NAME + '], '
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = @DestSchema
      AND TABLE_NAME   = @DestTable
      AND COLUMN_NAME <> @PKColumn;

    -- Trim trailing commas
    SET @col_list = LEFT(@col_list, LEN(@col_list) - 1);
    SET @upd_list = LEFT(@upd_list, LEN(@upd_list) - 1);

    SET @merge_sql = '
    MERGE [' + @DestSchema + '].[' + @DestTable + '] AS target
    USING [' + @StagingSchema + '].[' + @StagingTable + '] AS source
        ON target.[' + @PKColumn + '] = source.[' + @PKColumn + ']
    WHEN MATCHED AND source.CT_Operation = ''D'' THEN
        DELETE
    WHEN MATCHED AND source.CT_Operation IN (''U'', ''I'') THEN
        UPDATE SET ' + @upd_list + '
    WHEN NOT MATCHED BY TARGET AND source.CT_Operation <> ''D'' THEN
        INSERT ([' + @PKColumn + '], ' + @col_list + ')
        VALUES (source.[' + @PKColumn + '], ' + @col_list + ');
    ';

    EXEC sp_executesql @merge_sql;
END
```

---

## 7. Audit & Watermark Management

### On Success — CompleteSourceTableLoad

```sql
CREATE PROCEDURE [control].[CompleteSourceTableLoad]
    @TableConfigID  INT,
    @RowsLoaded     INT
AS
BEGIN
    UPDATE [control].[SourceTableConfig]
    SET LastLoadEnd     = GETUTCDATE(),
        LastLoadStatus  = 'Success',
        LastRowsLoaded  = @RowsLoaded,
        -- Advance watermark to current CT version
        LastSyncVersion = CHANGE_TRACKING_CURRENT_VERSION()
    WHERE TableConfigID = @TableConfigID;
END
```

### On Failure — CompleteSourceTableLoad_Failure

```sql
CREATE PROCEDURE [control].[CompleteSourceTableLoad_Failure]
    @TableConfigID  INT,
    @ErrorMessage   NVARCHAR(MAX)
AS
BEGIN
    -- Deliberately does NOT update LastSyncVersion
    -- Next run will re-attempt from the same watermark
    UPDATE [control].[SourceTableConfig]
    SET LastLoadEnd     = GETUTCDATE(),
        LastLoadStatus  = 'Failed',
        LastLoadError   = @ErrorMessage
    WHERE TableConfigID = @TableConfigID;
END
```

Preserving the watermark on failure ensures **no data loss** — the next pipeline run will re-process the same delta.

---

## 8. Parquet Source Handling

The 4th source delivers Parquet files to Azure Blob Storage on a scheduled basis. Since there is no Change Tracking available for files, an alternative incremental strategy was used:

- Files are organized in the container by date partition: `container/source_name/yyyy/MM/dd/HH/`
- ADF uses a **tumbling window trigger** parameter to construct the folder path for the current 15-minute window
- Only files in the current window's folder are read — acts as a natural watermark
- Data is normalized to match the staging schema of equivalent SQL Server tables before the MERGE step

---

## 9. Error Handling & Retry Logic

- Each table runs **independently** inside a ForEach activity — one table failing does not block others
- ADF Copy Data activity is configured with **2 retries, 30-second interval** for transient network errors
- On SP failure, ADF executes `CompleteSourceTableLoad_Failure` and marks the table — the next scheduled trigger picks it up automatically
- A monitoring query checks for any tables stuck in `'Running'` status beyond 30 minutes (indicator of a hung pipeline)

```sql
-- Monitoring query — stuck or failed loads
SELECT *
FROM [control].[SourceTableConfig]
WHERE LastLoadStatus IN ('Failed', 'Running')
  AND LastLoadStart < DATEADD(MINUTE, -30, GETUTCDATE());
```

---

## 10. Design Decisions

| Decision | Chosen | Alternative Considered | Reason |
|----------|--------|----------------------|--------|
| Change capture method | Change Tracking | CDC (Change Data Capture) | CT is lighter weight, no SQL Agent dependency |
| Pipeline pattern | Metadata-driven | Hardcoded per-table pipelines | Scalable — new tables need no code changes |
| Merge strategy | Dynamic SP | Static MERGE per table | Single SP handles all tables |
| Staging approach | Truncate + reload | Append with dedup | Simpler, staging is always clean delta |
| Latency | 15 minutes | Real-time streaming | Business requirement; CT sufficient |
| Failure handling | Preserve watermark | Reset watermark | Guarantees no data loss on retry |

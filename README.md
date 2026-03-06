# Incremental Delta Load Pipeline — Multi-Source to Azure SQL
### Azure Data Factory · SQL Server Change Tracking · Dynamic MERGE

![Azure](https://img.shields.io/badge/Azure-Data%20Factory-0078D4?style=flat&logo=microsoftazure)
![SQL Server](https://img.shields.io/badge/SQL%20Server-Change%20Tracking-CC2927?style=flat&logo=microsoftsqlserver)
![Azure SQL](https://img.shields.io/badge/Azure-SQL%20Database-0078D4?style=flat&logo=microsoftazure)
![Parquet](https://img.shields.io/badge/Format-Parquet-50ABF1?style=flat)

---

## Overview

A near-real-time incremental data ingestion pipeline that consolidates data from **4 heterogeneous on-premises sources** into a unified Azure SQL Database. The pipeline executes every **15 minutes**, using SQL Server Change Tracking to capture only new or modified records — avoiding costly full table scans on large production databases.

---

## Architecture

<img width="778" height="574" alt="image" src="https://github.com/user-attachments/assets/71527121-20d1-42e8-8d30-00e7145c6e84" />


---

## Pipeline Flow



<img width="1230" height="254" alt="image" src="https://github.com/user-attachments/assets/4a107f73-8fed-4ad0-8a66-7498aac3ade2" />



| Step | ADF Activity | Description |
|------|-------------|-------------|
| 1 | `GetTableMetaData` Lookup | Reads control table — drives dynamic, metadata-based execution |
| 2 | `StartSourceTableLoad` SP | Logs load start, retrieves last Change Tracking version |
| 3 | `Copy Table` Copy Data | Extracts delta rows using `CHANGETABLE()`, lands in staging |
| 4 | `Merge Staged Data` SP | Executes dynamic MERGE into persistent table |
| 5 ✅ | `CompleteSourceTableLoad` SP | Updates watermark, logs success |
| 5 ❌ | `CompleteSourceTableLoad_Failure` SP | Logs failure, preserves watermark for retry |

---

## Key Features

- **Incremental only** — Change Tracking captures inserts, updates, and deletes since last run
- **Metadata-driven** — adding a new source table requires zero pipeline changes, just a new row in the control table
- **Dynamic MERGE** — stored procedure auto-generates MERGE statements based on table schema
- **Fault tolerant** — per-table failure handling; one bad table doesn't block others
- **Full audit trail** — every load attempt logged with start time, end time, row counts, and status
- **Parquet support** — Blob Storage Parquet files normalized into the same staging schema as SQL sources

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Orchestration | Azure Data Factory |
| Source Tracking | SQL Server Change Tracking + `CHANGETABLE()` |
| File Source | Azure Blob Storage (Parquet) |
| Destination | Azure SQL Database |
| Transform | T-SQL Dynamic MERGE Stored Procedures |
| Scheduling | ADF Trigger — every 15 minutes |

---

## Repository Structure

```
├── adf/                        # ADF pipeline JSON exports
│   └── pipeline/
├── sql/
│   ├── change_tracking/        # Enable CT scripts per source table
│   ├── staging/                # Staging table DDL
│   ├── persistent/             # Persistent table DDL
│   └── stored_procedures/      # Merge, audit, watermark SPs
├── docs/
│   └── DETAILS.md              # Deep-dive technical documentation
└── README.md
```

---

## Documentation

For a full technical deep-dive including SQL examples, watermark logic, and design decisions, see **[DETAILS.md](docs/DETAILS.md)**.

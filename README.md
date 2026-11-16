# Earthquake Data Analytics & Geospatial Monitoring Platform

An end-to-end data engineering pipeline that ingests live earthquake data from the USGS Earthquake API into Azure Databricks, processes it through a Medallion Architecture (Bronze → Silver → Gold) using Delta Lake, and produces curated analytical tables for reporting and visualization.

![Python](https://img.shields.io/badge/Python-3.x-blue)
![PySpark](https://img.shields.io/badge/PySpark-Spark%20SQL-orange)
![Databricks](https://img.shields.io/badge/Azure%20Databricks-FF3621)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD8)
![USGS API](https://img.shields.io/badge/USGS-FDSN%20API-green)

## Overview

This project builds a production-style data pipeline around the [USGS (United States Geological Survey) Earthquake API](https://earthquake.usgs.gov/fdsnws/event/1/query). It ingests live earthquake events, stores the raw data in a Bronze Delta table, cleans and structures it into a Silver layer, and creates analytical Gold tables ready for dashboards and reporting.

The pipeline is designed around enterprise data engineering practices:

- **Incremental loading** using an event-time watermark, so each run fetches only new earthquakes
- **Delta Lake MERGE (upsert)** to prevent duplicate records while allowing updates
- **Data quality validation** at every layer (null checks, duplicate checks, coordinate validation)
- **Audit logging** of every pipeline run for traceability and monitoring

## Key Features

- Live ingestion from the USGS FDSN API (GeoJSON) with retry logic (3 attempts, 30-second timeout, 5-second backoff)
- Incremental loads driven by the latest processed event time; full load on first run (default start date: `2026-01-01`)
- Bronze / Silver / Gold Medallion Architecture on Delta Lake
- Explicit schema parsing of nested GeoJSON (event properties and geometry)
- Data quality checks: null values, duplicate earthquake IDs, invalid latitude/longitude ranges, post-MERGE record-count validation
- Pipeline audit log capturing run ID, duration, records read/written, and SUCCESS/FAILED status
- Six Gold aggregate tables for daily, monthly, hourly, regional, magnitude, and tsunami analysis
- Gold tables exported as CSV for downstream use

## Architecture

```text
USGS Earthquake API (GeoJSON)
        │
        ▼
01_API_Ingestion_Bronze ──► Bronze Delta Table (raw JSON)
        │
        ▼
02_Bronze_To_Silver ──► Silver Delta Table (cleaned, typed)
        │
        ▼
03_Silver_To_Gold ──► Gold Delta Tables (aggregates) ──► CSV export
        │
04_Data_Quality_Audit_Log ──► pipeline_audit_log (run history)
```

### Target Architecture with Azure Data Factory

```text
USGS Earthquake API (real-time earthquake data)
        │
        ▼
Azure Data Factory (pipeline orchestration and scheduling)
        │
        ▼
Azure Data Lake Storage Gen2 (cloud storage)
        │
        ▼
Bronze (raw data) ──► Silver (clean data) ──► Gold (analytics)
        │
        ▼
Power BI (earthquake analytics dashboard)
```

## Tech Stack

| Area | Technology |
| --- | --- |
| Language | Python 3, PySpark (Spark SQL) |
| Platform | Azure Databricks |
| Storage | Delta Lake |
| Data source | USGS FDSN Earthquake API (GeoJSON) |
| Libraries | `requests`, `pyspark.sql`, `delta.tables` |

## Key Terms

| Service | Purpose |
| --- | --- |
| Azure Data Factory | Orchestrate notebook execution |
| ADLS Gen2 | Store Bronze, Silver, and Gold data |
| Azure Databricks | Process data using PySpark |
| Delta Lake | Store transactional datasets |
| Unity Catalog | Manage tables, permissions, and governance |
| Databricks Workflows | Schedule and monitor jobs |
| Power BI | Visualize earthquake analytics |

> Azure Data Factory, ADLS Gen2, Unity Catalog, Databricks Workflows, and Power BI are part of the target architecture below. The pipeline as it stands today runs on Databricks with Delta Lake; these services are the planned integration.

## Project Structure

```text
EARTHQUAKE-DATA-ANALYTICS-GEOSPATIAL-MONITORING-PLATFORM/
├── README.md
├── common/
│   ├── config.py              # Central configuration: database, tables, API, retries
│   ├── common_utils.py        # Shared log() helper
│   ├── audit_utils.py         # Reserved for audit helpers
│   └── validation_utils.py    # Reserved for validation helpers
└── notebooks/
    ├── 01_API_Ingestion_Bronze.py
    ├── 02_Bronze_To_Silver.py
    ├── 03_Silver_To_Gold.py
    ├── 04_Data_Quality_Audit_Log.py
    └── gold_daily_earthquake_summary.csv   # Sample Gold output
```

## Roadmap — Planned Notebooks

The four notebooks above are the current pipeline. These notebooks are planned next:

| Notebook | Purpose |
| --- | --- |
| `05_Incremental_Load` | Process only newly arrived or changed earthquake records |
| `06_Delta_Merge_Upsert` | Implement Delta Lake MERGE operations |
| `07_Data_Quality_Framework` | Validate missing values, duplicates, and invalid records |
| `08_Pipeline_Monitoring` | Track execution status, duration, and failures |
| `09_Performance_Optimization` | Explore partitioning, file optimization, and query performance |
| `10_Gold_Analytics_SQL` | Create analytical SQL queries for reporting |

## Pipeline Walkthrough

### 01 — API Ingestion (Bronze)

- Checks whether the Bronze table already exists. First run performs a **full load** from the configured start date; later runs read the latest event time (watermark) from Bronze and request only newer events from the API.
- Builds the API URL dynamically (`format=geojson`, `orderby=time`) and fetches data with retries.
- Validates the API response (`features` and `metadata` keys), then stores each event as a raw record with `id`, `properties` and `geometry` as JSON strings, plus `source_system`, `api_endpoint`, `ingestion_timestamp` and `load_date`.
- Writes to the Bronze Delta table with a **MERGE upsert on event `id`** — new events are inserted, changed events are updated, duplicates are never created.
- Runs a post-MERGE validation that fails the run if any duplicate earthquake IDs exist.

### 02 — Bronze to Silver

- Parses the nested GeoJSON with explicit schemas for event properties (`mag`, `place`, `time`, `alert`, `tsunami`, `sig`, `status`, `net`, `magType`, …) and geometry (`coordinates`).
- Flattens the data into typed columns: `earthquake_id`, `magnitude`, `place`, `event_time`, `longitude`, `latitude`, `depth_km`, `tsunami`, `alert`, `significance`, `status`, `network`, `magnitude_type`, `event_type`, `title`.
- Converts epoch-millisecond timestamps to proper timestamps.
- Runs data quality checks: nulls in key columns, duplicate earthquake IDs, and invalid coordinates (latitude outside ±90, longitude outside ±180).
- Writes the cleaned data to `usgs_lakehouse.silver_earthquakes`.

### 03 — Silver to Gold

Builds six curated Gold tables in `usgs_lakehouse`:

| Gold table | Contents |
| --- | --- |
| `gold_daily_earthquake_summary` | Per-day earthquake count, avg / max / min magnitude, avg depth |
| `gold_magnitude_distribution` | Counts by category: Micro (<1), Minor (1–2), Light (2–3), Moderate (3–4), Strong (4–5), Major (≥5) |
| `gold_tsunami_summary` | Counts grouped by tsunami flag |
| `gold_region_summary` | Counts and magnitude stats grouped by place and coordinates |
| `gold_monthly_trend` | Year/month counts and magnitude stats |
| `gold_hourly_trend` | Hour-of-day counts and magnitude stats |

All six tables are exported as CSV files.

### 04 — Data Quality Audit Log

- `write_audit_log()` records every pipeline run to `usgs_lakehouse.pipeline_audit_log`, capturing pipeline name, run ID (UUID), layer, table, start/end time, duration, records read, records written, status (`SUCCESS` / `FAILED`), and error message.
- Provides a full run history for monitoring, debugging, and data traceability.

## Data Model

All tables live in the `usgs_lakehouse` database:

| Table | Layer | Description |
| --- | --- | --- |
| `bronze_earthquakes` | Bronze | Raw USGS events as JSON |
| `silver_earthquakes` | Silver | Cleaned, typed earthquake records |
| `gold_daily_earthquake_summary` | Gold | Daily aggregates |
| `gold_magnitude_distribution` | Gold | Counts by magnitude category |
| `gold_tsunami_summary` | Gold | Counts by tsunami flag |
| `gold_region_summary` | Gold | Aggregates by place and coordinates |
| `gold_monthly_trend` | Gold | Monthly aggregates |
| `gold_hourly_trend` | Gold | Hourly aggregates |
| `pipeline_audit_log` | Audit | Pipeline run history |

## Sample Output

`gold_daily_earthquake_summary.csv` (sample, July 1–6, 2026):

| event_date | total_earthquakes | avg_magnitude | max_magnitude | min_magnitude | avg_depth_km |
| --- | --- | --- | --- | --- | --- |
| 2026-07-04 | 310 | 1.64 | 5.2 | -0.17 | 21.11 |
| 2026-07-05 | 274 | 1.46 | 5.8 | -0.32 | 19.38 |
| 2026-07-03 | 236 | 1.44 | 6.2 | -0.88 | 19.61 |

## How to Run

**Prerequisites:** an Azure Databricks workspace with a cluster (Delta Lake support) and access to the USGS API.

1. Import the `notebooks/` and `common/` folders into your Databricks workspace (for example, under `USGS_Earthquake_Project/`).
2. Update the `%run` paths in `01_API_Ingestion_Bronze.py` to point to the `common/` folder in **your** workspace — they currently point to the author's workspace path.
3. Adjust `common/config.py` if needed (database name, default start date, retry settings).
4. Run the notebooks in order: `01` → `02` → `03` → `04`.
5. Query the Gold tables with Databricks SQL, or connect Power BI / a dashboard tool to the Gold layer.

## Project History

This pipeline is the Databricks rebuild of an earlier interactive dashboard project built with Python, Dash, and Plotly: [NaturalDisasters](https://github.com/Manish-Chhetr/NaturalDisasters). The dashboard version covers exploratory analysis, geospatial visualization, and a Random Forest regression model; this repo focuses on the data engineering pipeline behind it.

## Future Improvements

- Streaming ingestion with Auto Loader
- Alerting on high-magnitude events
- See the Roadmap above for the planned notebooks and the Azure Data Factory integration.

## Author

**Manish Chhetri** — [LinkedIn](https://www.linkedin.com/in/manish-chhetri/)

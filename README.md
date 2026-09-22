# Data Engineering Take-Home Assignment

A production-ready data engineering solution implementing robust SQL modeling, automated data cleaning, an idempotent ETL pipeline writing partitioned Parquet datasets, legacy code refactoring, and data warehouse architecture design.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Repository Structure](#repository-structure)
- [Key Features & Tasks](#key-features--tasks)
  - [Task 2: SQL & Relational Modeling (PostgreSQL)](#task-2-sql--relational-modeling-postgresql)
  - [Task 3: Python Data Processing & Pipeline](#task-3-python-data-processing--pipeline)
  - [Task 4: Data Warehouse & System Architecture](#task-4-data-warehouse--system-architecture)
  - [Task 5: Production Incident Triage](#task-5-production-incident-triage)
- [Prerequisites & Environment Setup](#prerequisites--environment-setup)
- [Usage & Execution](#usage--execution)
  - [1. Running the Python Pipeline (Task 3.2)](#1-running-the-python-pipeline-task-32)
  - [2. Using the Refactored Daily Aggregation (Task 3.3)](#2-using-the-refactored-daily-aggregation-task-33)
  - [3. Running SQL Queries (Task 2)](#3-running-sql-queries-task-2)
- [Design Decisions & Assumptions](#design-decisions--assumptions)
- [Documentation Index](#documentation-index)

---

## Project Overview

This repository contains the complete implementation and technical documentation for the Data Engineer take-home assignment. It addresses real-world data engineering challenges:
- Handling messy vendor payloads, malformed values, and non-standard decimals.
- Deduplicating deliveries with deterministic tie-breaking logic.
- Processing incremental updates and late-arriving events with replay grace windows.
- Building an idempotent, fault-tolerant Python ETL pipeline with partitioned Parquet storage and rollback capabilities.
- Designing dimensional models (Star Schema, SCD Type 2) and defining production debugging runbooks.

---

## Repository Structure

```text
.
├── data/                               # Sample/mock source data
│   ├── apps.csv                        # Apps dimension data
│   ├── campaign_costs.csv              # Marketing spend data
│   └── events_raw.csv                  # Raw incoming event deliveries
├── out/                                # Pipeline output directory
│   ├── events_clean/                   # Parquet partitioned by event_date (Hive style)
│   │   └── event_date=YYYY-MM-DD/
│   │       └── *.parquet
│   └── quarantine.parquet              # Malformed / unprocessable records
├── python/                             # Python source code
│   ├── pipeline/
│   │   ├── __init__.py
│   │   └── run.py                      # Task 3.2: CLI runner with atomic publish & rollback
│   ├── __init__.py
│   ├── clean_events.py                 # Task 3.1: Data cleaning, normalization & quarantine
│   └── task_3_3.py                     # Task 3.3: Refactored load_daily() function
├── sql/                                # SQL scripts (PostgreSQL dialect)
│   ├── task_2_1_deduplication.sql      # Task 2.1: Deterministic deduplication
│   ├── task_2_2_window_functions.sql   # Task 2.2: Running total, 7-day MA, DoD change
│   ├── task_2_3_joining_costs.sql      # Task 2.3: Full outer join revenue & costs, ROAS
│   ├── task_2_4_incremental_load.sql   # Task 2.4: Watermark + replay grace window upsert
│   └── task_2_5_data_quality.sql       # Task 2.5: Zero-row DQ assertion checks
├── AI_USAGE.md                         # Declaration of AI tooling usage
├── ANSWERS.md                          # Detailed written answers & explanations
├── ASSUMPTIONS.md                      # Technical assumptions & business rules
├── requirements.txt                    # Project Python dependencies (pandas, pyarrow)
└── README.md                           # Project documentation (this file)
```

---

## Key Features & Tasks

### Task 2: SQL & Relational Modeling (PostgreSQL)

Located in the [`sql/`](file:///e:/Prog/Projects/Data/sql) directory:

- **2.1 De-duplication** ([`task_2_1_deduplication.sql`](file:///e:/Prog/Projects/Data/sql/task_2_1_deduplication.sql)):
  - Retains one authoritative record per `event_id` using `ROW_NUMBER()`.
  - Order priority: `ingested_at DESC` (so vendor revenue corrections supersede original deliveries), parsed numeric `revenue_usd DESC`, and canonical payload hashing for fully deterministic ordering. Test traffic (`is_test`) is removed before ranking.
- **2.2 Window Functions** ([`task_2_2_window_functions.sql`](file:///e:/Prog/Projects/Data/sql/task_2_2_window_functions.sql)):
  - Computes daily revenue per app, cumulative running total from the first observed event date, 7-day moving average, and day-over-day percentage change (safely returning `NULL` when previous day revenue is 0).
- **2.3 Joining Costs** ([`task_2_3_joining_costs.sql`](file:///e:/Prog/Projects/Data/sql/task_2_3_joining_costs.sql)):
  - Joins revenue and spend at the grain of `(date, app_id, media_source, campaign)` using `FULL OUTER JOIN`.
  - Retains unconverted spend (cost without revenue) and organic / unmapped revenue (revenue without cost). Computes ROAS (`revenue / cost`), returning `NULL` when cost is zero.
- **2.4 Incremental Load** ([`task_2_4_incremental_load.sql`](file:///e:/Prog/Projects/Data/sql/task_2_4_incremental_load.sql)):
  - High-water mark tracking on `ingested_at` rather than `event_time` (ensuring revenue corrections are detected).
  - Incorporates a 90-minute replay grace window (`_REPLAY_GRACE_MINUTES`) for late-arriving deliveries, and upserts into `events_clean` using `ON CONFLICT (event_id) DO UPDATE`.
- **2.5 Data Quality Checks** ([`task_2_5_data_quality.sql`](file:///e:/Prog/Projects/Data/sql/task_2_5_data_quality.sql)):
  - Set-based assertion queries that return **zero rows on success**:
    1. Primary key uniqueness check (`event_id` duplicates in clean table -> pipeline blocker).
    2. Test data leakage check (`is_test = true` in clean table -> pipeline blocker).
    3. Referential integrity check (orphan `app_id` not found in `apps` dimension -> warning).

---

### Task 3: Python Data Processing & Pipeline

Located in the [`python/`](file:///e:/Prog/Projects/Data/python) directory:

- **3.1 Data Cleaning & Normalization** ([`clean_events.py`](file:///e:/Prog/Projects/Data/python/clean_events.py)):
  - **Revenue Parsing**: Normalizes string floats, handles comma decimals (`5,25` $\to$ `5.25`), and coerces `NULL`/malformed values to `0.0` while counting occurrences in `stats["revenue_parse_errors"]`.
  - **Country Code**: Trims and standardizes to 2-letter uppercase ASCII ISO codes; non-matching codes become `XX`.
  - **Timestamps**: Parsed into timezone-aware UTC timestamps.
  - **Test Filter**: Removes test traffic (`true`, `1`, `t`, `yes`).
  - **Quarantine Engine**: Malformed CSV lines (via `on_bad_lines`), records with missing mandatory identifiers (`event_id`, `user_id`, `app_id`), or unparseable timestamps are quarantined with explicit reason tags without aborting the batch.
  - **Deduplication**: Replicates the SQL ranking logic to keep the latest, highest-revenue record per `event_id`.

- **3.2 Idempotent Partitioned Pipeline** ([`pipeline/run.py`](file:///e:/Prog/Projects/Data/python/pipeline/run.py)):
  - Reads raw input CSV, cleans records, filters by `--since` date (UTC), and writes partitioned Parquet files (`event_date=YYYY-MM-DD`).
  - Writes to a temporary scratch directory first.
  - **Safe Publishing & Rollback**: Moves existing partition directories to sibling backup locations (`.event_date=...previous`), installs new partitions, cleans stale partitions on/after `--since`, and automatically rolls back to the previous state if any filesystem error occurs.
  - Generates a separate `quarantine.parquet` for unprocessable records.

- **3.3 Code Refactoring** ([`task_3_3.py`](file:///e:/Prog/Projects/Data/python/task_3_3.py)):
  - Refactored `load_daily(path, day)` preserving the original function signature:
    - Replaced slow `df.iterrows()` iteration with vectorized `groupby(["app_id", "media_source"])["revenue"].sum()`.
    - Eliminated fragile string-based date matching (`str.startswith(day)`) in favor of parsed UTC date filtering.
    - Reused Task 3.1 cleaning to ensure deduplication, test exclusion, and robust revenue parsing are uniform across jobs.

---

### Task 4: Data Warehouse & System Architecture

Detailed conceptual designs in [`ANSWERS.md`](file:///e:/Prog/Projects/Data/ANSWERS.md):
- **4.1 Star Schema**: Grain definition for `fact_campaign_daily` linked to `dim_date`, `dim_app`, `dim_campaign`, and `dim_media_source`.
- **4.2 Slowly Changing Dimensions**: Campaign name changes modeled using **SCD Type 2** (`is_current`, `valid_from`, `valid_to`, surrogate keys) to preserve historical attribution integrity.
- **4.3 Idempotency**: Business vs. engineering perspectives; strategies for zero-side-effect retries (partition overwrite, merge/upsert).
- **4.4 Late-Arriving Data**: Handling backfilled events, partition rewrites, replay windows, and auditable client communications.
- **4.5 Partitioning Strategy**: Analyzing partition pruning on `event_date` vs. operational queries on `ingested_at`.

---

### Task 5: Production Incident Triage

Systematic 5-step triage runbook in [`ANSWERS.md`](file:///e:/Prog/Projects/Data/ANSWERS.md) diagnosing missing Sunday revenue in client-facing dashboards:
1. Raw vendor ingestion verification (SDK / upstream file drop check).
2. Data cleaning & quarantine inspection (`is_test`, parse errors, dropped rows).
3. Data warehouse aggregate verification (timezone truncation, join drops).
4. Dashboard query, cache, and extract validation.
5. Cross-app comparison to identify blast radius (tenant-specific vs. global outage).

---

## Prerequisites & Environment Setup

### Prerequisites
- Python 3.10+ (tested on Python 3.11/3.12)
- PostgreSQL 12+ (for running SQL scripts)

### Installation

1. Clone or extract the repository:
   ```bash
   cd /path/to/repository
   ```

2. Create and activate a virtual environment:
   - **Linux / macOS**:
     ```bash
     python3 -m venv .venv
     source .venv/bin/activate
     ```
   - **Windows (PowerShell)**:
     ```powershell
     python -m venv .venv
     .\.venv\Scripts\Activate.ps1
     ```
   - **Windows (CMD)**:
     ```cmd
     python -m venv .venv
     .\.venv\Scripts\activate.bat
     ```

3. Install required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

---

## Usage & Execution

### 1. Running the Python Pipeline (Task 3.2)

To run the pipeline, navigate into the `python/` directory and execute `pipeline.run` as a module:

**Windows (PowerShell)**:
```powershell
cd python
python -m pipeline.run --input ../data/ --output ../out/ --since 2026-01-01
```

**Windows (CMD)**:
```cmd
cd python
python -m pipeline.run --input ../data/ --output ../out/ --since 2026-01-01
```

**Linux / macOS**:
```bash
cd python
python3 -m pipeline.run --input ../data/ --output ../out/ --since 2026-01-01
```

> **Note**: Running directly from the `python/` directory ensures Python automatically resolves the `pipeline` package and `clean_events` module without needing extra environment variable configurations.

**Output Summary**:
Upon completion, the script prints summary metrics and outputs:
- Clean partitioned files: `out/events_clean/event_date=YYYY-MM-DD/*.parquet`
- Quarantined records: `out/quarantine.parquet` (if any malformed records were present)

### 2. Using the Refactored Daily Aggregation (Task 3.3)

```python
import sys
from pathlib import Path
sys.path.append("python")

from task_3_3 import load_daily

# Returns aggregated revenue grouped by app_id and media_source for the given UTC date
df = load_daily("data/events_raw.csv", "2026-01-05")
print(df)
# Output columns: ["key", "revenue"] sorted by revenue descending
```

### 3. Running SQL Queries (Task 2)

All SQL files in [`sql/`](file:///e:/Prog/Projects/Data/sql) are written in standard PostgreSQL dialect:
- Connect to your database using `psql` or any SQL client (DBeaver, DataGrip):
  ```bash
  psql -U postgres -d your_database -f sql/task_2_1_deduplication.sql
  psql -U postgres -d your_database -f sql/task_2_2_window_functions.sql
  psql -U postgres -d your_database -f sql/task_2_3_joining_costs.sql
  psql -U postgres -d your_database -f sql/task_2_4_incremental_load.sql
  psql -U postgres -d your_database -f sql/task_2_5_data_quality.sql
  ```

---

## Design Decisions & Assumptions

Key technical assumptions made across the implementation (see [`ASSUMPTIONS.md`](file:///e:/Prog/Projects/Data/ASSUMPTIONS.md) for complete details):
- **Tie-Breaking Rule**: When duplicate `event_id` records have identical `ingested_at` values, numeric `revenue_usd` is evaluated descending, followed by a canonical string representation of all payload attributes to guarantee deterministic output across runs.
- **Grace Window**: The incremental SQL load uses a 90-minute lookback window against `max(ingested_at)` to absorb delayed batch arrivals.
- **Atomic File Swaps**: The pipeline uses directory-level atomic renames and sibling `.previous` backups so concurrent readers or failed job executions never leave the dataset in a partially written or corrupted state.
- **Zero-Row Assertions**: Data quality checks are structured as assertion queries where an empty result set denotes 100% pass, making integration into orchestration tools (Airflow, Dagster, dbt test) seamless.

---

## Documentation Index

- [`ANSWERS.md`](file:///e:/Prog/Projects/Data/ANSWERS.md) — Comprehensive technical responses to all assignment questions (Tasks 2–5).
- [`ASSUMPTIONS.md`](file:///e:/Prog/Projects/Data/ASSUMPTIONS.md) — Complete list of business and engineering assumptions.
- [`AI_USAGE.md`](file:///e:/Prog/Projects/Data/AI_USAGE.md) — Transparent disclosure of AI assistance utilized during development.

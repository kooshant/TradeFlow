# TradeFlow – Global Trade Intelligence Platform

TradeFlow is an end-to-end data engineering and analytics platform
that transforms public international trade data into structured,
validated, analytics-ready datasets.

The project currently focuses on India's 2023 imports and exports
using official United Nations UN Comtrade data.

It demonstrates API-based batch ingestion, data validation, distributed
processing with Apache Spark, Delta Lake storage, reference data
enrichment, analytical data modelling, SQL analytics, and interactive
visualization.

## Project Highlights

- Processes 165,485 public trade records.
- Covers 13,048 export records and 152,437 import records.
- Implements Raw, Silver, Curated, Enriched, and Gold data layers.
- Uses PySpark for large-scale data transformations.
- Uses Delta Lake for reliable analytical storage.
- Enriches trade records with official HS 2022 commodity and
  partner-area reference data.
- Builds five analytics marts for business analysis.
- Implements four SQL analytics queries using DuckDB.
- Provides an interactive Streamlit dashboard.
- Includes automated data validation and project tests.

## Current Project Scope

| Attribute | Value |
|---|---|
| Reporter | India |
| Reporter ISO | IND |
| Reference year | 2023 |
| Commodity classification | HS 2022 |
| Detailed commodity level | HS 6-digit |
| Primary data source | UN Comtrade |
| Processing type | Batch |
| Raw records | 165,485 |
| Export records | 13,048 |
| Import records | 152,437 |

## Technology Stack

- **Language:** Python
- **Data processing:** Pandas, PySpark
- **Storage:** CSV, JSON, Parquet, Delta Lake
- **Data source:** UN Comtrade API
- **Reference enrichment:** HS 2022 and partner-area reference data
- **SQL analytics:** DuckDB, SQL
- **Analytics:** Python, PySpark, DuckDB
- **Visualization:** Streamlit, Plotly
- **Testing:** Pytest
- **Version control:** Git and GitHub
- **Containerization:** Docker

## Architecture

TradeFlow follows a layered batch data engineering architecture.

```text
                 UN Comtrade API
                        |
                        v
                Python Ingestion
                        |
                        v
                   RAW LAYER
                Consolidated JSON
                        |
                        v
                DATA VALIDATION
                        |
                        v
                  SILVER LAYER
              Cleaned and Standardized
                        |
                        v
                 APACHE SPARK
                        |
                        v
                 CURATED LAYER
                     Parquet
                        |
                        v
                  DELTA LAKE
                        |
                        v
              REFERENCE ENRICHMENT
              HS and Partner Dimensions
                        |
                        v
                   GOLD LAYER
          +-------------+-------------+
          |             |             |
          v             v             v
       National       Product       Import
       Summary       Analytics     Partner
                                  Analytics
          |             |             |
          +-------------+-------------+
                        |
                        v
                 ANALYTICS MARTS
                        |
              +---------+---------+
              |                   |
              v                   v
        DUCKDB SQL            STREAMLIT
         ANALYTICS            DASHBOARD
              |                   |
              v                   v
        SQL RESULTS             PLOTLY
```

## Installation and Setup

### Prerequisites

- Python 3.12
- Java 17
- Git
- Docker (for containerized execution)

Java 17 is required for the local Apache Spark environment.

### 1. Clone the repository

```bash
git clone https://github.com/kooshant/TradeFlow.git
cd TradeFlow
```

### 2. Create a virtual environment

```bash
python3.12 -m venv .venv
source .venv/bin/activate
```

### 3. Install dependencies

```bash
python -m pip install --upgrade pip
pip install -r requirements-dev.txt
```

### 4. Configure environment variables

Create a `.env` file in the project root:

```env
COMTRADE_API_KEY=your_comtrade_api_key
```

Replace the placeholder with your own UN Comtrade API key. Never commit your `.env` file or expose API credentials publicly.

### 5. Run the dashboard

After the pipelines have generated the required analytics marts, launch the dashboard:

```bash
streamlit run src/tradeflow/dashboard.py --server.port 8505
```

Open http://localhost:8505 in your browser.

## SQL Analytics Layer

TradeFlow uses DuckDB to execute analytical SQL queries directly against Gold-layer Parquet datasets.

Four SQL queries are currently implemented:

| SQL File | Purpose |
|---|---|
| `01_national_trade_overview.sql` | National exports, imports and indicative trade balance |
| `02_product_trade_analytics.sql` | Top 10 export and import products |
| `03_import_partner_concentration.sql` | Import partner rankings and cumulative concentration |
| `04_top_partner_product_mix.sql` | Product mix across leading import partners |

The SQL results are validated through automated tests. The partner-product query is also compared against the existing analytics mart.

## Running Tests

Run the complete automated test suite:

```bash
python -m pytest -v
```

The tests cover API connectivity, analytics mart validation, dashboard loading, SQL query execution, and SQL-to-mart consistency.

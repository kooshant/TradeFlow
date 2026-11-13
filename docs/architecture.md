# TradeFlow — System Architecture

## 1. Project Overview

TradeFlow is a batch-oriented Global Trade Intelligence Platform designed to process and analyze India's international merchandise trade data.

It uses public data from UN Comtrade and transforms raw trade records into validated, enriched and analytics-ready datasets.

### Current Coverage

- Reporter: India
- Reference year: 2023
- Commodity classification: HS 2022
- Commodity detail: Six-digit HS level
- Processing mode: Batch

## 2. System Architecture

```mermaid
flowchart TD
    A["UN Comtrade API"] --> B["Raw JSON Ingestion"]
    B --> C["Raw Data Validation"]
    C --> D["Silver Layer - CSV"]
    D --> E["PySpark Transformation"]
    E --> F["Curated Parquet Layer"]
    F --> G["Delta Lake Tables"]

    H["UN Comtrade Reference Data"] --> I["Reference Validation"]
    I --> J["Enriched Dimensions"]

    G --> K["Gold Analytics Layer"]
    J --> K

    K --> L["Analytics Mart Builder"]
    L --> M["Parquet Analytics Marts"]

    M --> N["Streamlit Dashboard"]
    M --> O["Automated Data Quality Tests"]

    cat >> docs/architecture.md <<'EOF'
## 3. Technology Stack

| Component | Technology |
|---|---|
| Programming | Python |
| Data Processing | Apache Spark / PySpark |
| Data Storage | Parquet |
| Lakehouse | Delta Lake |
| Data Source | UN Comtrade API |
| Analytics | Pandas |
| Dashboard | Streamlit |
| Visualization | Plotly |
| Testing | Pytest |
| Version Control | Git / GitHub |

## 4. Data Pipeline Layers

### Raw Layer
- Retrieves India's import and export records from UN Comtrade.
- Stores original API responses in JSON.
- Keeps export and import data separately.

### Silver Layer
- Consolidates import and export records.
- Standardizes the data structure.
- Derives HS commodity hierarchy information.
- Checks duplicates and invalid records.

### Curated Layer
- Uses PySpark for distributed processing.
- Processes HS six-digit commodity records.
- Separates national trade summaries from partner-level records.
- Stores structured Parquet datasets.

### Delta Lake Layer
- Stores curated datasets as Delta tables.
- Supports transaction history and time travel.
- Enforces schemas.
- Supports MERGE operations.

### Reference Enrichment
- Uses official UN Comtrade commodity reference data.
- Enriches HS commodity descriptions.
- Enriches partner area descriptions.
- Creates reusable commodity and partner dimensions.

### Gold Layer
Creates three business-oriented datasets:
1. National Trade Flow Summary
2. Product Trade Analytics
3. Import Partner Analytics

Validates record counts, rankings, business keys, shares and source-to-Gold reconciliation.

### Analytics Marts
Creates five dashboard-ready datasets:
1. National Trade KPIs
2. Top Export Products
3. Top Import Products
4. Import Partner Concentration
5. Top Partner Product Mix

### Dashboard
Streamlit and Plotly present:
- National export and import KPIs.
- Leading export products.
- Leading import products.
- Import partner concentration.
- Partner-level product analysis.

## 5. Data Quality and Testing

Automated tests currently cover:
- UN Comtrade API connectivity.
- Dashboard execution and KPI presence.
- Analytics mart availability.
- Expected record counts.
- Product ranking integrity.
- Partner shares and rankings.
- Partner-product data quality.

Run tests using:

    python -m pytest -v

## 6. Important Business Considerations

### Trade Valuation
- Exports use FOB valuation.
- Imports use CIF valuation.
- These valuation bases differ.
- Their difference should not be interpreted as a fully comparable economic trade balance.

### Partner Interpretation
The partner2 field represents the secondary partner / country of consignment in the source data. It should not automatically be interpreted as the country of origin.

### National and Partner Records
World aggregate records and partner-level records must not be added together because this can double-count trade.

### Commodity Quantities
Quantity units can differ between commodities. Quantities should not be summed across unrelated commodity codes without unit normalization.

### Trend Analysis
The current implementation covers 2023 only. It does not support year-over-year growth or multi-year trend analysis yet.

## 7. Current Implementation Status

TradeFlow currently provides a validated batch-processing pipeline for India's 2023 international trade data.

Future expansion can include:
- Additional reporting years.
- Cloud-based storage.
- Automated pipeline orchestration.
- Broader trade intelligence analytics.
- Additional data quality controls.

## 8. Project Objectives

TradeFlow demonstrates practical Data Engineering capabilities:
- API-based data ingestion.
- Data validation and transformation.
- Distributed processing with PySpark.
- Lakehouse storage with Delta Lake.
- Reference data enrichment.
- Dimensional analytics.
- Analytics mart development.
- Interactive business intelligence.
- Automated testing.

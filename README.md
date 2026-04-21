# Saudi RE Market Intelligence Dashboard 🏠

**Saudi Real Estate Market Intelligence: Seasonal Patterns, Emerging Cities & Deal Size Trends (2020–2025)**

An end-to-end data pipeline and bilingual (Arabic/English) dashboard analysing 1.4 million real estate transactions from Saudi Arabia's Ministry of Justice (MOJ).

> Course project for [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) 2026.

---

## Problem Description

Saudi Arabia's real estate market sees dramatic quarterly variation, rapid growth in secondary cities, and shifting deal sizes across property types. This project answers three questions:

1. **Seasonal Patterns** — Which quarters consistently outperform across the 13 Saudi regions?
2. **Emerging Cities** — Which of 175 cities show the highest transaction growth rates (CAGR)?
3. **Deal Size Trends** — Are median deal values and property areas growing or shrinking by classification?

### Dataset

[Saudi Real Estate Open Data](https://github.com/civillizard/Saudi-Real-Estate-Data) — 1.41 million MOJ sale transactions across 13 regions, 175 cities, 24 quarters (2020–2025).

---

## Architecture

```
GitHub Repo (sparse checkout)
    │
    ├── moj/sales/*.csv (24 files, 1.4M rows)
    └── data/region_mapping.csv
            │
      ┌─────┴──────┐
      │   Prefect   │ ← Batch ingestion pipeline
      │  (4 tasks)  │    Download → Clean → Parquet → DuckDB
      └─────┬──────┘
            │
      ┌─────┴──────┐
      │   DuckDB    │ ← Local data warehouse
      │  (local)    │    1.4M rows, optimized schema
      └─────┬──────┘
            │
      ┌─────┴──────┐
      │  dbt-duckdb │ ← Transformations
      │  (4 models) │    staging + 3 mart models
      └─────┬──────┘
            │
      ┌─────┴──────┐
      │  Streamlit  │ ← Dashboard (4 tiles, bilingual)
      │  (deployed) │    [deployed URL]
      └────────────┘
```

## Tech Stack

| Component | Technology |
|-----------|-----------|
| **Data Ingestion** | Prefect (batch pipeline, 4-task DAG) |
| **Data Lake** | Local Parquet files |
| **Data Warehouse** | DuckDB (local, single-file) |
| **Transformations** | dbt-duckdb (staging + 3 marts) |
| **Dashboard** | Streamlit + Plotly (4 tiles, AR/EN) |
| **Reproducibility** | Docker, Makefile, `.env.example` |

---

## Dashboard

The dashboard has **4 interactive tiles** with a **bilingual toggle** (Arabic 🇸🇦 / English 🇬🇧) and interactive sidebar filters (Region, Year, Top N):

> **Live Dashboard (Deployed):** [https://saudi-re-market.streamlit.app/](https://saudi-re-market.streamlit.app/) *(add actual URL here after deployment)*

### Tile 1: Seasonal Patterns
Average quarterly transactions by region (Q1-Q4 × 13 regions), filterable by year.
*(Insert Screenshot Here)*
`![Tile 1 - Seasonal Heatmap](insert_link_here)`

### Tile 2: Emerging Cities
Top N cities ranked by transaction CAGR, colored by region.
*(Insert Screenshot Here)*
`![Tile 2 - Emerging Cities](insert_link_here)`

### Tile 3: Median Deal Price Trend
Quarterly median price by property classification.
*(Insert Screenshot Here)*
`![Tile 3 - Median Price](insert_link_here)`

### Tile 4: Median Area Trend
Quarterly median area by property classification.
*(Insert Screenshot Here)*
`![Tile 4 - Median Area](insert_link_here)`

---

## How to Reproduce

### Prerequisites

- Python 3.9+
- Git
- ~500 MB disk space

### Option 1: Makefile (Recommended)

```bash
git clone <this-repo>
cd DEProject2026
pip install -r requirements.txt

# Run everything in one command:
make all

# Then start the dashboard:
make dashboard
```

### Option 2: Step by Step

```bash
# 1. Clone this repo
git clone <this-repo>
cd DEProject2026

# 2. Install dependencies
pip install -r requirements.txt

# 3. Clone the dataset (sparse checkout)
make clone

# 4. Run ingestion pipeline
python3 flows/ingest_pipeline.py

# 5. Run dbt transformations
cd dbt/saudi_re
dbt seed --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
cd ../..

# 6. Export marts for dashboard
make export

# 7. Start dashboard
cd streamlit && streamlit run app.py
```

### Option 3: Docker

```bash
docker compose up --build
make dashboard
```

---

## Project Structure

```
DEProject2026/
├── flows/
│   └── ingest_pipeline.py       # Prefect DAG (4 tasks)
├── dbt/saudi_re/
│   ├── models/staging/
│   │   └── stg_moj_sales.sql    # Staging: clean + enrich
│   ├── models/marts/
│   │   ├── fct_seasonal_patterns.sql
│   │   ├── fct_emerging_cities.sql
│   │   └── fct_deal_size_trends.sql
│   └── seeds/
│       └── region_mapping.csv
├── streamlit/
│   ├── app.py                   # Dashboard (4 tiles, bilingual)
│   ├── fct_*.csv                # Pre-computed mart data
│   └── .streamlit/config.toml
├── data/                        # Generated (gitignored)
│   ├── raw/                     # Source CSVs
│   ├── lake/                    # Parquet files
│   └── saudi_re.duckdb          # DWH
├── Dockerfile
├── docker-compose.yml
├── Makefile
├── requirements.txt
└── README.md
```

## Data Warehouse Design

**Table: `raw.moj_sales`** — 1,412,119 rows

The table includes `year_quarter` as a key column, enabling partition-like query pruning in DuckDB. All three analysis models filter/group by this column and by `region`, making these the optimal indexing dimensions.

| Column | Type | Description |
|--------|------|-------------|
| region_ar / region_en | VARCHAR | Canonical region names (13 regions) |
| city | VARCHAR | City name (172 cities) |
| date_gregorian | DATE | Transaction date |
| year_quarter | INTEGER | E.g., 202001 — key partition column |
| property_classification | VARCHAR | سكني/تجاري/زراعي/صناعي |
| price | DOUBLE | Price in SAR |
| area | DOUBLE | Area in m² |

## dbt Models

| Model | Type | Description |
|-------|------|-------------|
| `stg_moj_sales` | Staging | Adds English labels, price_per_m², filters invalid rows |
| `fct_seasonal_patterns` | Mart | Avg quarterly metrics by region across all years |
| `fct_emerging_cities` | Mart | City CAGR rankings (min 10 txns, 2+ years) |
| `fct_deal_size_trends` | Mart | Median price/area/price_per_m² by quarter × classification |

**dbt tests:** 13 tests (not_null, accepted_values) — all passing ✅

---

## Data Source & Attribution

- **Source:** [Saudi Real Estate Open Data](https://github.com/civillizard/Saudi-Real-Estate-Data) by [@civillizard](https://github.com/civillizard)
- **Original data:** Ministry of Justice (MOJ), Saudi National Open Data Portal
- **License:** [KSA Open Data License](https://github.com/civillizard/Saudi-Real-Estate-Data/blob/main/LICENSE-DATA.md)

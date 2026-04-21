"""
Saudi RE Market Intelligence — Prefect Ingestion Pipeline
Reads MOJ Sales CSVs → Cleans → Parquet Data Lake → DuckDB DWH
"""
import os
import re
import glob
from pathlib import Path

import duckdb
import pandas as pd
from prefect import flow, task


# ─── Configuration ────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / os.getenv("RAW_DATA_PATH", "data/raw/saudi-re-data")
SALES_DIR = RAW_DIR / "moj" / "sales"
MAPPING_FILE = RAW_DIR / "data" / "region_mapping.csv"
LAKE_DIR = BASE_DIR / os.getenv("LAKE_PATH", "data/lake")
DUCKDB_PATH = BASE_DIR / os.getenv("DUCKDB_PATH", "data/saudi_re.duckdb")

# Only quarterly sales CSVs (skip index files)
SALES_PATTERN = "MOJ-Sales-*.csv"

# ─── Standard schema (2020-2022, 2024-2025 + 2023 Q2-Q4) ─────
STANDARD_COLS = {
    "المنطقة": "region_ar",
    "المدينة": "city_raw",
    "المدينة / الحي": "city_district",
    "الرقم المرجعي للصفقة": "transaction_ref",
    "تاريخ الصفقة ميلادي": "date_gregorian",
    "تاريخ الصفقة هجري": "date_hijri",
    "تصنيف العقار": "property_classification",
    "عدد العقارات": "property_count",
    "السعر": "price",
    "المساحة": "area",
    "نوع العقار": "property_type",  # only in 2023 Q2-Q3
}

# 2023-Q1 has a completely different schema
Q1_2023_COLS = {
    "رقم مرجعي": "transaction_ref",
    "المنطقة": "region_ar",
    "المدينة": "city_raw",
    "الحي": "district",
    "المخطط": "plat",
    "رقم القطعة": "parcel",
    "التاريخ": "date_gregorian",
    "تصنيف العقار": "property_classification",
    "نوع العقار": "property_type",
    "عدد العقارات": "property_count",
    "السعر بالريال السعودي": "price",
    "المساحة": "area",  # stripped (original has trailing space)
    "سعر المتر المربع": "price_per_m2_original",
}

# Target columns for unified output
OUTPUT_COLS = [
    "region_ar", "region_en", "city", "district",
    "date_gregorian", "year", "quarter", "year_quarter",
    "property_classification", "property_type",
    "property_count", "area", "price",
    "source_file",
]


# ─── Task 1: Load Region Mapping ──────────────────────────────────
@task(name="load_region_mapping", log_prints=True)
def load_region_mapping() -> dict:
    """Load the region_mapping.csv and build variant → canonical dicts."""
    df = pd.read_csv(MAPPING_FILE)
    ar_map = dict(zip(df["variant"], df["canonical_ar"]))
    en_map = dict(zip(df["variant"], df["canonical_en"]))
    # Also map canonical_ar → canonical_en
    ar_to_en = dict(zip(df["canonical_ar"], df["canonical_en"]))
    print(f"Loaded {len(ar_map)} region variants → {len(ar_to_en)} canonical regions")
    return {"ar_map": ar_map, "en_map": en_map, "ar_to_en": ar_to_en}


# ─── Task 2: Read and Clean CSVs ──────────────────────────────────
@task(name="clean_and_merge", log_prints=True)
def clean_and_merge(region_maps: dict) -> pd.DataFrame:
    """Read all 24 quarterly CSVs, clean and merge into one DataFrame."""

    csv_files = sorted(glob.glob(str(SALES_DIR / SALES_PATTERN)))
    print(f"Found {len(csv_files)} sales CSV files")

    frames = []
    for filepath in csv_files:
        filename = os.path.basename(filepath)
        print(f"  Processing: {filename}")

        # Detect schema variant
        is_2023_q1 = "2023-Q1" in filename

        # Read CSV
        df = pd.read_csv(filepath, dtype=str, encoding="utf-8-sig")

        if is_2023_q1:
            df = _process_2023_q1(df, filename)
        else:
            df = _process_standard(df, filename)

        frames.append(df)

    # Concatenate all
    merged = pd.concat(frames, ignore_index=True)
    print(f"Total rows after concat: {len(merged):,}")

    # ── Region normalization ──
    ar_map = region_maps["ar_map"]
    ar_to_en = region_maps["ar_to_en"]
    merged["region_ar"] = merged["region_ar"].str.strip().map(ar_map).fillna(merged["region_ar"])
    merged["region_en"] = merged["region_ar"].map(ar_to_en).fillna("Unknown")

    # ── Filter invalid rows ──
    before = len(merged)
    merged = merged[merged["price"] > 0]
    merged = merged[merged["area"] > 0]
    print(f"Filtered {before - len(merged):,} rows with price/area <= 0. Remaining: {len(merged):,}")

    # ── Select output columns ──
    for col in OUTPUT_COLS:
        if col not in merged.columns:
            merged[col] = None
    merged = merged[OUTPUT_COLS]

    return merged


def _clean_numeric(series: pd.Series) -> pd.Series:
    """Remove comma separators and convert to float."""
    return pd.to_numeric(series.str.replace(",", "", regex=False), errors="coerce")


def _parse_dates(series: pd.Series) -> pd.Series:
    """Parse dates handling both YYYY/MM/DD and M/D/YYYY formats."""
    # Try YYYY/MM/DD first (most common)
    parsed = pd.to_datetime(series, format="%Y/%m/%d", errors="coerce")
    # Fallback to M/D/YYYY
    mask = parsed.isna()
    if mask.any():
        parsed[mask] = pd.to_datetime(series[mask], format="%m/%d/%Y", errors="coerce")
    # Final fallback: let pandas infer
    mask = parsed.isna()
    if mask.any():
        parsed[mask] = pd.to_datetime(series[mask], errors="coerce")
    return parsed


def _process_standard(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    """Process standard-schema CSVs (all except 2023 Q1)."""
    # Strip BOM and whitespace from column names
    df.columns = df.columns.str.strip().str.replace("\ufeff", "")

    # Rename columns
    rename_map = {k: v for k, v in STANDARD_COLS.items() if k in df.columns}
    df = df.rename(columns=rename_map)

    # Clean numerics
    df["price"] = _clean_numeric(df["price"])
    df["area"] = _clean_numeric(df["area"])

    # Parse dates
    df["date_gregorian"] = _parse_dates(df["date_gregorian"])

    # Extract city and district from city_district
    if "city_district" in df.columns:
        split = df["city_district"].str.split(r"[/／]", n=1, expand=True)
        df["city"] = split[0].str.strip() if 0 in split.columns else df.get("city_raw", "")
        df["district"] = split[1].str.strip() if 1 in split.columns else ""
    else:
        df["city"] = df.get("city_raw", "")
        df["district"] = ""

    # Derived columns
    df["year"] = df["date_gregorian"].dt.year
    df["quarter"] = df["date_gregorian"].dt.quarter
    df["year_quarter"] = df["year"] * 100 + df["quarter"]

    # Property count
    if "property_count" in df.columns:
        df["property_count"] = pd.to_numeric(df["property_count"], errors="coerce").fillna(1).astype(int)
    else:
        df["property_count"] = 1

    # Property type (only in 2023 Q2-Q3)
    if "property_type" not in df.columns:
        df["property_type"] = None

    df["source_file"] = filename
    return df


def _process_2023_q1(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    """Process 2023 Q1 with its unique schema."""
    df.columns = df.columns.str.strip().str.replace("\ufeff", "")

    rename_map = {k: v for k, v in Q1_2023_COLS.items() if k in df.columns}
    df = df.rename(columns=rename_map)

    # Clean numerics (no commas in 2023 Q1, but be safe)
    df["price"] = _clean_numeric(df["price"])
    df["area"] = _clean_numeric(df["area"])

    # Parse date (M/D/YYYY format)
    df["date_gregorian"] = _parse_dates(df["date_gregorian"])

    # City is already a separate column; district is in "district" col
    df["city"] = df.get("city_raw", "").str.strip()
    if "district" in df.columns:
        df["district"] = df["district"].str.strip()
    else:
        df["district"] = ""

    # Derived columns
    df["year"] = df["date_gregorian"].dt.year
    df["quarter"] = df["date_gregorian"].dt.quarter
    df["year_quarter"] = df["year"] * 100 + df["quarter"]

    if "property_count" in df.columns:
        df["property_count"] = pd.to_numeric(df["property_count"], errors="coerce").fillna(1).astype(int)
    else:
        df["property_count"] = 1

    df["source_file"] = filename
    return df


# ─── Task 3: Save to Data Lake (Parquet) ──────────────────────────
@task(name="save_to_lake", log_prints=True)
def save_to_lake(df: pd.DataFrame) -> Path:
    """Save cleaned DataFrame as Parquet to the data lake."""
    LAKE_DIR.mkdir(parents=True, exist_ok=True)
    out_path = LAKE_DIR / "moj_sales.parquet"
    df.to_parquet(out_path, index=False, engine="pyarrow")
    size_mb = out_path.stat().st_size / 1024 / 1024
    print(f"Saved {len(df):,} rows to {out_path} ({size_mb:.1f} MB)")
    return out_path


# ─── Task 4: Load to DuckDB ───────────────────────────────────────
@task(name="load_to_duckdb", log_prints=True)
def load_to_duckdb(parquet_path: Path) -> None:
    """Load Parquet data into DuckDB as the raw warehouse table."""
    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(DUCKDB_PATH))

    # Create schema
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")

    # Drop and recreate
    con.execute("DROP TABLE IF EXISTS raw.moj_sales")
    con.execute(f"""
        CREATE TABLE raw.moj_sales AS
        SELECT * FROM read_parquet('{parquet_path}')
    """)

    # Verify
    count = con.execute("SELECT COUNT(*) FROM raw.moj_sales").fetchone()[0]
    regions = con.execute("SELECT COUNT(DISTINCT region_en) FROM raw.moj_sales").fetchone()[0]
    cities = con.execute("SELECT COUNT(DISTINCT city) FROM raw.moj_sales").fetchone()[0]
    year_range = con.execute("SELECT MIN(year), MAX(year) FROM raw.moj_sales").fetchone()

    print(f"DuckDB loaded: {count:,} rows")
    print(f"  Regions: {regions}, Cities: {cities}")
    print(f"  Years: {year_range[0]}–{year_range[1]}")

    con.close()


# ─── Main Flow ─────────────────────────────────────────────
@flow(name="saudi-re-ingestion", log_prints=True)
def ingest_pipeline():
    """Main orchestration flow: Clean → Lake → DuckDB."""
    print("=" * 60)
    print("Saudi RE Market Intelligence — Ingestion Pipeline")
    print("=" * 60)

    # Step 1: Load region mapping
    region_maps = load_region_mapping()

    # Step 2: Clean and merge all CSVs
    df = clean_and_merge(region_maps)

    # Step 3: Save to data lake (Parquet)
    parquet_path = save_to_lake(df)

    # Step 4: Load to DuckDB
    load_to_duckdb(parquet_path)

    print("=" * 60)
    print("Pipeline complete!")
    print("=" * 60)


if __name__ == "__main__":
    ingest_pipeline()

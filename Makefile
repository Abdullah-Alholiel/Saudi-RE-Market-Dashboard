.PHONY: clone ingest transform export dashboard all clean

# Clone the dataset (sparse checkout — only sales + mapping data)
clone:
	@echo "=== Cloning Saudi RE data (sparse checkout) ==="
	mkdir -p data/raw/saudi-re-data
	cd data/raw/saudi-re-data && \
		git init && \
		git remote add origin https://github.com/civillizard/Saudi-Real-Estate-Data.git && \
		git sparse-checkout init --cone && \
		git sparse-checkout set moj/sales data && \
		git pull --depth 1 origin main

# Run the Prefect ingestion pipeline
ingest:
	@echo "=== Running ingestion pipeline ==="
	python3 flows/ingest_pipeline.py

# Run dbt seed + transformations + tests
transform:
	@echo "=== Running dbt transformations ==="
	cd dbt/saudi_re && \
		dbt seed --profiles-dir . && \
		dbt run --profiles-dir . && \
		dbt test --profiles-dir .

# Export mart tables as CSVs for Streamlit Cloud
export:
	@echo "=== Exporting mart data for dashboard ==="
	python3 -c "\
import duckdb; \
con = duckdb.connect('data/saudi_re.duckdb', read_only=True); \
[con.execute(f'SELECT * FROM main.{t}').fetchdf().to_csv(f'streamlit/{t}.csv', index=False) \
 for t in ['fct_seasonal_patterns','fct_emerging_cities','fct_deal_size_trends']]; \
con.close(); \
print('Exported!')"

# Run the Streamlit dashboard locally
dashboard:
	@echo "=== Starting Streamlit dashboard ==="
	cd streamlit && streamlit run app.py

# Full pipeline (assumes data not yet cloned)
all: clone ingest transform export
	@echo "=== Pipeline complete! Run 'make dashboard' to view ==="

# Clean generated data
clean:
	rm -rf data/raw data/lake data/*.duckdb data/*.duckdb.wal
	rm -rf dbt/saudi_re/target dbt/saudi_re/dbt_packages dbt/saudi_re/logs

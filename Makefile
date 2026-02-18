PYTHON ?= python3
.PHONY: setup benchmark notebook clean download check-connections

# Check cloud connections and download data
setup: check-connections download

# Verify connectivity to all hosted services
check-connections:
	@echo "Checking cloud database connections..."
	@$(PYTHON) -c "import sys; sys.path.insert(0,'.'); from lib.connections import verify_connections; s=verify_connections(); [print(f'  {k}: OK') for k,v in s.items() if v]; sys.exit(0 if all(s.values()) else 1)"
	@echo "All connections verified."

# Download dataset
download:
	$(PYTHON) scripts/download.py

# Run all ingestion benchmarks
benchmark:
	@echo "=== Running DuckDB/MotherDuck ingestion ==="
	$(PYTHON) scripts/ingest_duckdb.py
	@echo ""
	@echo "=== Running PostgreSQL/Neon ingestion ==="
	$(PYTHON) scripts/ingest_postgres.py
	@echo ""
	@echo "=== Running Graph (PostgreSQL) ingestion ==="
	$(PYTHON) scripts/ingest_graph.py
	@echo ""
	@echo "=== Running AXYM ingestion (placeholder) ==="
	$(PYTHON) scripts/ingest_axym.py
	@echo ""
	@echo "All benchmarks complete. Results in results/"

# Launch Jupyter notebook
notebook:
	jupyter lab notebooks/step_1_data_ingestion.ipynb

# Remove data files
clean:
	rm -f data/*.parquet data/*.csv data/*.csv.zip data/*.partial
	@echo "Cleaned data files."

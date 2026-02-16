.PHONY: setup benchmark notebook clean download infra wait-healthy

# Start Docker containers, wait for health, download data
setup: infra wait-healthy download

# Start Docker infrastructure
infra:
	docker compose up -d

# Wait for databases to be healthy
wait-healthy:
	@echo "Waiting for PostgreSQL..."
	@python -c "import sys; sys.path.insert(0,'.'); from lib.connections import wait_for_postgres; sys.exit(0 if wait_for_postgres(60) else 1)"
	@echo "PostgreSQL is ready."
	@echo "Waiting for Neo4j..."
	@python -c "import sys; sys.path.insert(0,'.'); from lib.connections import wait_for_neo4j; sys.exit(0 if wait_for_neo4j(120) else 1)"
	@echo "Neo4j is ready."

# Download dataset
download:
	python scripts/download.py

# Run all ingestion benchmarks
benchmark:
	@echo "=== Running DuckDB ingestion ==="
	python scripts/ingest_duckdb.py
	@echo ""
	@echo "=== Running PostgreSQL ingestion ==="
	python scripts/ingest_postgres.py
	@echo ""
	@echo "=== Running Neo4j ingestion ==="
	python scripts/ingest_neo4j.py
	@echo ""
	@echo "=== Running AXYM ingestion (placeholder) ==="
	python scripts/ingest_axym.py
	@echo ""
	@echo "All benchmarks complete. Results in results/"

# Launch Jupyter notebook
notebook:
	jupyter lab notebooks/step_1_data_ingestion.ipynb

# Remove data files and Docker volumes
clean:
	rm -f data/*.parquet data/*.csv data/*.csv.zip data/*.duckdb data/*.partial
	rm -rf data/neo4j_import
	docker compose down -v
	@echo "Cleaned data files and Docker volumes."

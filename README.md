Data Engineering Zoomcamp - ETL Pipeline

Overview
- Fetches NYC taxi trip data (CSV/CSV.GZ/Parquet) in chunks, normalizes schema and loads into Postgres.
- Fetches and Loads taxi zone lookup data once into `taxi_zone_lookup`.
- Includes Docker Compose for Postgres + pgAdmin.

Requirements
- Python 3.13+
- Docker (optional, for local Postgres/pgAdmin)

Setup
1) Create and activate a virtual environment.
2) Install dependencies:
```powershell
pip install -r requirements.txt
```
If you use `uv` or `pip`, dependencies are listed in `pyproject.toml`.

3) Set environment variables in `.env`


Docker (optional)
```powershell
docker compose up -d
```
Postgres runs on `localhost:5432`
pgAdmin on `http://localhost:8080`.

Run ETL
```powershell
python src/etl_pipeline.py
```

One-time zone lookup load
```powershell
python src/one_time_load.py
```

Code Layout
- `src/etl_pipeline.py`: End-to-end ETL for trip data (chunked extraction, normalization, transform, load).
- `src/one_time_load.py`: Loads taxi zone lookup table.
- `src/utils.py`: Postgres helpers and table schemas.
- `docker-compose.yaml`: Local Postgres + pgAdmin.

Notes
- The ETL pipeline supports CSV, CSV.GZ, and Parquet inputs.
- Make sure `POSTGRES_ENGINE_URI` and database names align with the DB used in `src/etl_pipeline.py` and `src/one_time_load.py`.
- Note: All metadata and the data dictionary are available at `https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page`.

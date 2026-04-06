@echo off
echo ========================================
echo   Open Fabric - Starting All Services
echo ========================================

echo.
echo [1/5] Starting Docker services...
docker-compose up -d
timeout /t 30 /nobreak

echo.
echo [2/5] Generating data...
python data_generator/generate.py

echo.
echo [3/5] Ingesting to MinIO...
python ingestion/ingest.py

echo.
echo [4/5] Running dbt models...
cd dbt_project\insurance_dwh
dbt run
cd ..\..

echo.
echo [5/5] Copying DuckDB to Superset...
docker cp dbt_project/insurance_dwh/insurance.duckdb superset:/tmp/insurance.duckdb
docker exec -u root -it superset chmod 777 /tmp/insurance.duckdb

echo.
echo ========================================
echo   All services started successfully!
echo.
echo   Airflow:  http://localhost:8081
echo   Superset: http://localhost:8088
echo   MinIO:    http://localhost:9001
echo ========================================
pause
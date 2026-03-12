# project_junior_data_eng

## 📂 Project Structure

```text
project_junior_data_eng/
├── data/
│   └── input/               # Drop location for source CSV files
├── db/
│   └── schema.sql           # Initial table creation scripts (if any)
├── src/
│   ├── main.py              # Pipeline entry point
│   ├── extract.py           # Handles reading data from sources
│   ├── transform.py         # Data cleansing 
│   ├── load.py              # Database insertion with idempotency
│   └── helper.py            # Shared utilities and DB connections
├── docker-compose.yml       # Orchestrates PostgreSQL and the Python app
├── Dockerfile               # Instructions to build the Python environment
├── requirements.txt         # Python dependencies
└── README.md
```
## How to Run (On-Demand)
```text
docker-compose up --build
```
## How to Confirm Results
Connect to the running database:
```text
docker exec -it brokerage_db psql -U postgres -d brokerage
```
SQL
```text
SELECT * FROM trades LIMIT 5;
```

## Key Design Decisions & Trade-offs
```text
- Idempotency (Safe Re-runs): The pipeline uses a "Delete-then-Insert" strategy inside a database transaction for the trades table. This prevents duplicate data if the script is run multiple times for the same day.

- Data Cleansing: Standardizes strings (lowercase, stripped whitespace) and cleans numeric fields (removes commas).

- Referential Integrity: Filters out trades referencing invalid client_id or instrument_id before loading them into the database.

- Resilience: Uses a Docker healthcheck to ensure the Python container waits for PostgreSQL to be fully ready, preventing connection race conditions.
```
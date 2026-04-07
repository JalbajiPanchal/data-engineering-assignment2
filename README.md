# Data Engineering Pipeline — Assignment 2

## Task Overview
A complete data engineering pipeline integrating Delta Lake, Apache Spark, ScyllaDB, and Apache Airflow, containerized using Docker Compose.

## Architecture
```
[Data Generation] → [Delta Lake] → [Spark ETL] → [ScyllaDB]
                          ↑
                 [Airflow Orchestration]
```

## Tech Stack
| Technology | Version | Purpose |
|------------|---------|---------|
| Apache Spark | 3.5.0 | Data processing and transformation |
| Delta Lake | 3.0.0 | Data storage with ACID transactions and versioning |
| ScyllaDB | 5.4 | High-performance target database |
| Apache Airflow | 2.8.1 | Workflow orchestration and scheduling |
| PostgreSQL | 13 | Airflow metadata database |
| Docker Compose | Latest | Container orchestration |

## Project Structure
```
assignment2/
├── data/
│   ├── delta-lake/
│   │   └── customer_transactions/    <- Delta Lake storage
│   ├── transactions.csv              <- Generated sample data (1110 records)
│   └── sample_output.csv            <- Expected output data
├── scripts/
│   ├── generate_data.py             <- Data generation script (Faker)
│   └── delta_versioning.py          <- Delta Lake versioning demo
├── spark_jobs/
│   └── etl_job.py                   <- Spark ETL job
├── dags/
│   └── pipeline_dag.py              <- Airflow DAG definition
├── scylladb/
│   └── init.cql                     <- ScyllaDB schema initialization
├── docker-compose.yml               <- All services configuration
└── README.md
```

## Data Pipeline Flow
```
1. generate_data.py
   └── Creates 1110 records (CSV)
       ├── 1000 normal records
       ├── 50 duplicate records
       ├── 50 negative amount records
       └── 10 zero amount records

2. Spark ETL (etl_job.py)
   ├── Step 1: Read CSV → Write to Delta Lake
   ├── Step 2: Read from Delta Lake (1110 records)
   ├── Step 3: Remove duplicates        → 1060 records
   ├── Step 4: Filter invalid amounts   → 1000 records
   ├── Step 5: Add transaction_date column
   ├── Step 6: Aggregate daily totals per customer
   └── Step 7: Write 968 rows → ScyllaDB

3. ScyllaDB
   └── Stores daily_customer_totals table (968 rows)
```

## Input Data Format
```csv
transaction_id,customer_id,amount,timestamp,merchant
d74b5cda-f254-4ac0-b053-9493fa15ac8a,C00001,150.36,2025-01-15T10:30:00Z,STORE_23
```

| Column | Type | Description |
|--------|------|-------------|
| transaction_id | String | Unique transaction identifier (UUID) |
| customer_id | String | Customer identifier (C00001 - C00050) |
| amount | Float | Transaction amount |
| timestamp | Timestamp | Transaction timestamp (UTC) |
| merchant | String | Merchant name |

## Output Data Format
```csv
customer_id,transaction_date,daily_total
C00001,2025-01-28,66.49
C00001,2025-02-27,456.76
```

| Column | Type | Description |
|--------|------|-------------|
| customer_id | String | Customer identifier |
| transaction_date | Date | Date of transactions |
| daily_total | Float | Sum of all valid transactions for that day |

---

## Setup Instructions

### Prerequisites
- Docker Desktop installed and running
- Python 3.x installed
- Git installed
- 8GB RAM recommended

### Step 1 — Generate Sample Data
```cmd
pip install faker
python scripts\generate_data.py
```

Expected output:
```
Generated 1110 records → data/transactions.csv
- Normal records   : 1000
- Duplicates added : 50
- Negative amounts : 50
- Zero amounts     : 10
```

### Step 2 — Start All Docker Services
```cmd
docker-compose up -d
```

Wait 2-3 minutes for all services to fully start.

### Step 3 — Verify All Containers Running
```cmd
docker-compose ps
```

Expected output:
```
assignment2-postgres-1           healthy
assignment2-scylladb-1           running
assignment2-spark-master-1       running
assignment2-spark-worker-1       running
assignment2-airflow-init-1       running
assignment2-airflow-webserver-1  running
assignment2-airflow-scheduler-1  running
```

### Step 4 — Initialize ScyllaDB Schema
```cmd
docker exec -it assignment2-scylladb-1 cqlsh
```

Run inside cqlsh:
```sql
CREATE KEYSPACE IF NOT EXISTS transactions_ks
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE transactions_ks;

CREATE TABLE IF NOT EXISTS daily_customer_totals (
    customer_id TEXT,
    transaction_date DATE,
    daily_total FLOAT,
    PRIMARY KEY (customer_id, transaction_date)
);

DESCRIBE TABLES;
EXIT;
```

### Step 5 — Copy Data to Spark Containers
```cmd
docker cp data\transactions.csv assignment2-spark-master-1:/opt/spark/scripts/transactions.csv
docker exec -u root assignment2-spark-worker-1 mkdir -p /opt/spark/scripts
docker cp data\transactions.csv assignment2-spark-worker-1:/opt/spark/scripts/transactions.csv
```

### Step 6 — Run Spark ETL Job
```cmd
docker exec assignment2-spark-master-1 /opt/spark/bin/spark-submit ^
  --master spark://spark-master:7077 ^
  --packages io.delta:delta-spark_2.12:3.0.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 ^
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension ^
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog ^
  --conf spark.cassandra.connection.host=scylladb ^
  /opt/spark/spark_jobs/etl_job.py
```

Expected output:
```
INFO:ETL_Job: Loaded 1110 raw records
INFO:ETL_Job: Delta Lake table created!
INFO:ETL_Job: Read 1110 records from Delta Lake
INFO:ETL_Job: After dedup: 1060 records
INFO:ETL_Job: After filter: 1000 records
INFO:ETL_Job: Aggregated: 968 rows
INFO:ETL_Job: Data written to ScyllaDB!
INFO:ETL_Job: ETL Job Complete!
```

### Step 7 — Verify Data in ScyllaDB
```cmd
docker exec -it assignment2-scylladb-1 cqlsh
```

```sql
USE transactions_ks;
SELECT COUNT(*) FROM daily_customer_totals;
SELECT * FROM daily_customer_totals LIMIT 10;
```

Expected:
```
 count
-------
   968
```

### Step 8 — Delta Lake Versioning Demo
```cmd
docker cp scripts\delta_versioning.py assignment2-spark-master-1:/opt/spark/scripts/delta_versioning.py

docker exec assignment2-spark-master-1 /opt/spark/bin/spark-submit ^
  --packages io.delta:delta-spark_2.12:3.0.0 ^
  /opt/spark/scripts/delta_versioning.py
```

### Step 9 — Access Airflow UI
- URL: http://localhost:8085
- Username: admin
- Password: admin
- Enable DAG: customer_transaction_pipeline
- Click Trigger DAG to run manually

---

## Port Mappings
| Service | Host Port | Description |
|---------|-----------|-------------|
| Airflow UI | 8085 | DAG management interface |
| Spark Master UI | 8081 | Spark cluster monitoring |
| Spark Master | 7078 | Spark submit port |
| ScyllaDB | 9042 | CQL native transport |

---

## ScyllaDB Schema
```sql
CREATE KEYSPACE IF NOT EXISTS transactions_ks
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS daily_customer_totals (
    customer_id TEXT,
    transaction_date DATE,
    daily_total FLOAT,
    PRIMARY KEY (customer_id, transaction_date)
);
```

- customer_id: Partition key
- transaction_date: Clustering key
- daily_total: Aggregated transaction amount

---

## Data Quality Handling
| Issue | Count | Action |
|-------|-------|--------|
| Duplicate transaction_id | 50 | Removed - keep first occurrence |
| Negative amounts | 50 | Filtered out (amount <= 0) |
| Zero amounts | 10 | Filtered out (amount <= 0) |
| Valid records | 1000 | Processed and aggregated |
| Final output rows | 968 | Written to ScyllaDB |

---

## Airflow DAG Tasks
```
check_delta_lake
      ↓
check_scylladb_connection
      ↓
run_spark_etl
      ↓
verify_scylladb_data
      ↓
log_completion
```

| Task | Description |
|------|-------------|
| check_delta_lake | Verifies Delta Lake directory exists |
| check_scylladb_connection | Tests ScyllaDB port connectivity |
| run_spark_etl | Executes the full Spark ETL job |
| verify_scylladb_data | Confirms row count in ScyllaDB |
| log_completion | Logs pipeline completion time |

---

## Troubleshooting

### Port already allocated error
```cmd
docker-compose down
docker-compose up -d
```

### ScyllaDB connection refused
Wait 60 seconds after startup for ScyllaDB to fully initialize, then retry.

### Spark CSV file not found
```cmd
docker cp data\transactions.csv assignment2-spark-master-1:/opt/spark/scripts/transactions.csv
docker exec -u root assignment2-spark-worker-1 mkdir -p /opt/spark/scripts
docker cp data\transactions.csv assignment2-spark-worker-1:/opt/spark/scripts/transactions.csv
```

### Delta Lake version mismatch error
Use delta-spark_2.12:3.0.0 for Spark 3.5.0. Do not use delta-core.

### Airflow DAG not showing
Wait 2-3 minutes for the scheduler to detect the DAG file automatically.

### ivy2 cache permission error
```cmd
docker exec -u root assignment2-spark-master-1 mkdir -p /home/spark/.ivy2/cache
docker exec -u root assignment2-spark-master-1 chmod -R 777 /home/spark/.ivy2
```

### Airflow login not working
Default credentials are admin/admin set during airflow-init startup.
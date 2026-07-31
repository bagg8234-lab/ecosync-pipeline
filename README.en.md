# ⚡ EcoSync — Real-time Renewable Energy Trading Data Pipeline

**🌐 Language:** [한국어](README.md) | [English](README.en.md)

A data pipeline that brokers real-time energy trading between solar energy prosumers and consumers, optimizing energy efficiency through data collection, validation, and matching.

---

## Design Philosophy

Built on a **Local Validation → Cloud Migration** strategy.

1. **Environment Independence** — Docker ensures identical execution regardless of OS
2. **Logic First** — Fully validate pipeline logic with small datasets before moving to cloud
3. **Zero Code Change Migration** — Only `.env` connection settings change between local and Azure
4. **Infrastructure as Code** — Terraform manages Azure resources for reproducible environments

> Running cloud resources with flawed logic is wasteful.  
> Validate locally first, then deploy the proven code to the cloud.

---

## Branch Structure

| Branch | Environment | Description |
| :--- | :--- | :--- |
| `main` | Local (Docker) | Kafka + MinIO + PostgreSQL |
| `azure` | Cloud (Azure) | Event Hubs + ADLS Gen2 + Azure PostgreSQL |

---

## Screenshots

### Streamlit Dashboard
![dashboard1](docs/images/dashboard1.png)
![dashboard2](docs/images/dashboard2.png)
![dashboard3](docs/images/dashboard3.png)

### Kafka UI
![kafka-ui](docs/images/kafka-ui.png)

---

## Architecture

```
[KPX Solar Generation API]     [Demand Dummy Data]
        ↓                        ↓
   [Kafka Producer]──────────────┘
        ↓
      [Kafka]  (generation / demand topics)
        ↓
  [Pipeline Consumer]
        ↓
   [Pydantic]          ← Individual record validation (type/range/required fields)
   ↙              ↘
[data_error DLQ]   [Buffer accumulation (batches of 10)]
(manual review)          ↓
                   [Great Expectations]  ← Batch-level statistical validation (distribution/outliers)
                   ↙              ↘
            [data_error DLQ]   [PostgreSQL + MinIO]
            (manual review)          ↓
                                [Matching Engine]      ← Haversine distance-based
                                     ↓
                            [Dynamic Pricing]  ← KMA weather API + KPX SMP
                                     ↓
                           [Streamlit Dashboard]

     ┌─────────────────────────────────────────────────┐
     │      [DLQ Reprocessor] ← Cron (hourly, on the hour) │
     │   Consumes the dead-letter topic                  │
     │   ├─ error_type = data_error  → Skip (log only)   │
     │   └─ error_type = system_error → Revalidate (Pydantic)│
     │        └─ On pass, republish to original topic     │
     │           (generation/demand) → back to Kafka       │
     │              (re-enters pipeline from the start)   │
     └─────────────────────────────────────────────────┘

[DB/Storage write failure]
   ↙                                  ↘
Connection error                   Other (constraint violations, etc.)
(OperationalError,                 (NotNullViolation,
 EndpointConnectionError)           ClientError, etc.)
   ↓                                    ↓
[system_error DLQ]                [data_error DLQ]
(absorbed by the DLQ                (manual review)
 Reprocessor above)

[GE validation execution failure] → [system_error DLQ] (entire batch of records)
```

---

## Tech Stack

| Category | Local (Docker) | Cloud (Azure) |
| :--- | :--- | :--- |
| Message Broker | Kafka + Kafka UI | Azure Event Hubs (Kafka-compatible) |
| Storage | MinIO (S3-compatible) | ADLS Gen2 |
| Database | PostgreSQL v16 | Azure Database for PostgreSQL |
| Visualization | Streamlit / Tableau | Streamlit + Power BI |
| IaC | Docker Compose | Terraform |

---

## Key Features

**1. Data Integrity Validation**
- **Stage 1 (per-record)** Pydantic — Type/range/required field validation (negative generation, null values)
- **Stage 2 (batch of 10)** Great Expectations — Statistical anomaly detection across the full distribution (can catch records that already passed Pydantic)
- Failed validation at either stage → `data_error` DLQ isolation (manual review, no retry)
- **Stage 3 (storage)** DB/Storage write failures are classified by exception type
  - Connection errors (`psycopg2.OperationalError`, `botocore.EndpointConnectionError`) → recoverable on retry, so classified as `system_error`
  - Other exceptions (`NotNullViolation`, `ClientError`, etc. — constraint or configuration issues) → will fail identically on retry, so classified as `data_error`
  - If the GE validation call itself fails (environment/resource issue), the entire batch is treated as a transient problem and classified as `system_error`
  - This classification logic is verified by 6 mock-based unit tests (`tests/test_process_ge_batch.py`)
- DLQ Reprocessor — runs hourly via Cron; only `system_error` records are revalidated and republished to their original topic, re-entering the pipeline from the start (`data_error` records are skipped)

**2. Real-time Trade Matching Engine**
- Distance calculation using Haversine formula
- Nearest supplier priority matching
- Automatic fallback to next candidate when supply is insufficient
- Failed match log stored separately

**3. Dynamic Pricing**
- Korea Meteorological Administration API (real-time solar radiation/temperature)
- KPX SMP real data integration (replaced fixed 150 KRW → actual market price)
- Price = SMP × solar radiation factor × supply/demand ratio × temperature correction

**4. Real Data Integration**
- KPX solar generation API (by region and hour)
- KPX SMP (System Marginal Price) API
- Demand data uses dummy data due to lack of public API

---

## Data Mart

### Why I built this

The existing Streamlit dashboard queried `generation`/`demand` independently using a "most recent 50 rows" window to calculate the supply-demand ratio. However, since the two collectors run as independent processes, the time ranges covered by each "most recent 50 rows" could drift apart — meaning the dashboard could end up comparing data from two different time windows as if they were the same moment. As more dashboards were added (Streamlit + Power BI), there was also a growing risk that each dashboard would implement its own aggregation logic slightly differently.

To address this, I introduced a mart table, `daily_city_trading_summary`, that pre-aggregates the four raw tables (`generation`/`demand`/`trades`/`matching_errors`) at the (date, city) grain.

### Structure

```
generation ─┐
demand     ─┼─→ Batch aggregation (UPSERT) ─→ daily_city_trading_summary ─→ All dashboards query only this table
trades     ─┤
matching_errors ─┘
```

| Column | Description |
|---|---|
| `summary_date`, `city` | Aggregation key (PK) |
| `total_generation_kwh`, `total_demand_kwh` | Daily total generation/demand |
| `matched_trade_count`, `matched_kwh` | Successfully matched trade count/kWh |
| `unmatched_error_count`, `unmatched_kwh` | Failed match count/kWh |
| `match_rate_kwh_pct`, `match_rate_count_pct` | Match rate (computed once in the mart, reused by every dashboard) |

I used `FULL OUTER JOIN` + `COALESCE` to safely handle cases where only some of the raw tables had data for a given day, defaulting missing measures to 0. When the denominator of the match rate is 0, it's set to `NULL` instead of raising a division error.

### What I discovered while building it

1. **The matching engine doesn't only use "that day's" generation** — it also draws on previously accumulated, unmatched generation "inventory," so trades can occur even on a day with zero recorded generation.
2. **A data freshness issue in the KPX generation API** — the public data portal listed the update cycle as "real-time," but when I actually inspected the raw API response, it consistently returned finalized data delayed by 2–3 days, regardless of the request time. (See [Velog post] for the full investigation and root cause.)

### How to run it

```bash
# 1) Create the mart table (one-time)
docker exec -it ecosync-app python src/create_mart_table.py

# 2) Backfill historical data (one-time)
docker exec -it ecosync-app python src/run_daily_mart_batch.py --backfill 30

# 3) Re-run for a specific date
docker exec -it ecosync-app python src/run_daily_mart_batch.py --date 2026-07-31

# Going forward: schedule as a daily batch job (cron recommended)
docker exec -it ecosync-app python src/run_daily_mart_batch.py
```

---

## Database Schema

![erd](docs/images/erd.png)

| Table | Description |
| :--- | :--- |
| `generation` | Solar generation raw data |
| `demand` | Demand data |
| `trades` | Matched trade records |
| `matching_errors` | Failed match logs |

---

## Azure Migration (azure branch)

Migrated from local Docker to Azure cloud.  
Only `.env` connection settings need to be changed — no code modifications required.  
Azure resources are provisioned via Terraform.

### Azure Resources

**Event Hubs** — Kafka-compatible message broker (generation / demand / dead-letter)

![eventhub](docs/images/eventhub.png)

**Event Hubs Monitoring** — Real-time message throughput

![eventhubs](docs/images/eventhubs.png)

**ADLS Gen2** — Raw data lake (demand / generation stored by date)

![storage](docs/images/storage.png)

**Azure Database for PostgreSQL**

![db](docs/images/db.png)

### Infrastructure (Terraform)

```bash
cd terraform
terraform init
terraform plan
terraform apply
```

### Power BI Dashboard

Connected Power BI Desktop to Azure PostgreSQL for operational monitoring.

![powerbi](docs/images/powerbi.png)

### Pipeline Execution Logs

**Producer — Publishing data to Event Hubs**

![producer](docs/images/producer실행로그.png)

**Pipeline — Validation, Storage, and Matching Engine**

![pipeline](docs/images/pipeline실행로그.png)

### Troubleshooting

- **Event Hubs Basic tier** → Kafka protocol not supported (`NoBrokersAvailable`) → Upgraded to Standard
- **DLQ infinite loop** → Resolved by classifying `data_error` vs `system_error`

---

## Project Structure

```
ecosync-project/
├── .env.example
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── terraform/
│   ├── main.tf
│   └── variables.tf
├── sql/                 
│   ├── daily_city_trading_summary.sql
│   └── daily_city_trading_summary_upsert.sql
├── logs/
│   └── dlq.log
├── docs/
│   └── images/
└── src/
    ├── data_generator.py
    ├── kafka_producer.py
    ├── kafka_consumer.py
    ├── validator.py
    ├── ge_validator.py
    ├── dead_letter_queue.py
    ├── dlq_reprocessor.py
    ├── minio_client.py
    ├── db_client.py
    ├── matching_engine.py
    ├── weather_api.py
    ├── smp_api.py
    ├── generation_api.py
    ├── dynamic_pricing.py
    ├── pipeline.py
    ├── create_mart_table.py     
    ├── run_daily_mart_batch.py    
    ├── get_all_city_prices.py 
    └── dashboard.py
```

---

## Getting Started

### Local (Docker)

```bash
cp .env.example .env
docker-compose up -d
```

Terminal 1:
```bash
docker exec -it ecosync-app python src/pipeline.py
```

Terminal 2:
```bash
docker exec -it ecosync-app python src/kafka_producer.py
```

Dashboard:
```bash
docker exec -it ecosync-app streamlit run src/dashboard.py --server.address=0.0.0.0
```

Kafka UI: `http://localhost:8080`  
Streamlit: `http://localhost:8501`

### Azure (azure branch)

```bash
git checkout azure
cp .env.example .env
# Add Azure connection info to .env
```

Terminal 1:
```bash
python src/pipeline.py
```

Terminal 2:
```bash
python src/kafka_producer.py
```

DLQ Reprocessor:
```bash
python src/dlq_reprocessor.py
```

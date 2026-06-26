# ⚡ EcoSync — Real-time Renewable Energy Trading Data Pipeline

**🌐 Language:** [한국어](README.md) | [English](README.en.md)

A data pipeline that brokers real-time energy trading between solar energy prosumers and consumers, optimizing energy efficiency through data collection, validation, and matching.

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
[KPX Real Generation API]     [Demand Dummy Data]
        ↓                            ↓
   [Kafka Producer]─────────────────┘
        ↓
      [Kafka]          ← Real-time message queue
        ↓
  [Pipeline Consumer]
        ↓
   [Validator]         ← Pydantic + Great Expectations
   ↙              ↘
[data_error DLQ]   [PostgreSQL + MinIO]
(Manual Review)  ↙         ↘
           Success     [system_error DLQ]
                            ↓
                    [DLQ Reprocessor]  ← Cron (every 1 hour)
                            ↓
                    [Matching Engine]  ← Haversine distance-based
                            ↓
                    [Dynamic Pricing]  ← Weather API + KPX SMP
                            ↓
                   [Streamlit Dashboard]
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
- Pydantic — Type/range/required field validation (negative generation, null values)
- Great Expectations — Statistical anomaly detection
- Failed validation → `data_error` DLQ isolation (manual review)
- DB/Storage errors → `system_error` DLQ isolation (auto retry)
- DLQ Reprocessor — Only `system_error` retried automatically via Cron

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
├── terraform/              ← Azure IaC
│   ├── main.tf
│   └── variables.tf
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
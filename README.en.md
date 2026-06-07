# ⚡ EcoSync — Real-time Renewable Energy Trading Data Pipeline

**🌐 Language:** [한국어](README.md) | [English](README.en.md)

A serverless data pipeline that brokers real-time energy trading between solar energy prosumers and consumers, optimizing energy efficiency through data collection, validation, and matching.

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
   ↙         ↘
[DLQ]      [PostgreSQL + MinIO]
(Failed)     (Raw + Processed data)
  ↓
[DLQ Reprocessor]     ← Cron (every 1 hour)
                ↓
         [Matching Engine]  ← Haversine distance-based
                ↓
       [Dynamic Pricing]    ← Weather API + KPX SMP
                ↓
      [Streamlit Dashboard]
```

---

## Tech Stack

| Category | Local (Docker) | Cloud (Azure) |
| :--- | :--- | :--- |
| Orchestrator | Python Script | Azure Data Factory |
| Compute | Docker (Python 3.12-alpine) | Azure Functions |
| Message Broker | Kafka + Kafka UI | Azure Event Hubs |
| Storage | MinIO (S3-compatible) | ADLS Gen2 |
| Database | PostgreSQL v16-alpine | Azure SQL Database |
| Visualization | Streamlit / Tableau | Power BI |
| IaC | Docker Compose | Terraform |

---

## Key Features

**1. Data Integrity Validation**
- Pydantic — Type/range/required field validation (negative generation, null values)
- Great Expectations — Statistical anomaly detection (distribution validation)
- Failed data → Dead Letter Queue isolation
- DLQ Reprocessor — Automatic retry every 1 hour via Cron

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

## Project Structure

```
ecosync-project/
├── .env.example            # Environment variable template
├── docker-compose.yml      # Infrastructure definition
├── Dockerfile              # App container build
├── requirements.txt        # Python libraries
├── logs/
│   └── dlq.log             # DLQ reprocessor log
├── docs/
│   └── images/             # Screenshots
└── src/
    ├── data_generator.py   # Dummy data generation (demand)
    ├── kafka_producer.py   # Kafka publisher
    ├── kafka_consumer.py   # Kafka consumer (for testing)
    ├── validator.py        # Pydantic validation
    ├── ge_validator.py     # Great Expectations validation
    ├── dead_letter_queue.py # DLQ isolation
    ├── dlq_reprocessor.py  # DLQ reprocessor (Cron)
    ├── minio_client.py     # MinIO storage
    ├── db_client.py        # PostgreSQL UPSERT
    ├── matching_engine.py  # Trade matching
    ├── weather_api.py      # KMA Weather API
    ├── smp_api.py          # KPX SMP API
    ├── generation_api.py   # KPX Generation API
    ├── dynamic_pricing.py  # Price calculation
    ├── pipeline.py         # End-to-End pipeline
    └── dashboard.py        # Streamlit dashboard
```

---

## Getting Started

### 1. Set up environment variables

```bash
cp .env.example .env
# Add your API keys to .env
```

### 2. Start infrastructure

```bash
docker-compose up -d
```

### 3. Run pipeline

Terminal 1 — Pipeline:
```bash
docker exec -it ecosync-app python src/pipeline.py
```

Terminal 2 — Data publishing:
```bash
docker exec -it ecosync-app python src/kafka_producer.py
```

### 4. Run dashboard

```bash
docker exec -it ecosync-app streamlit run src/dashboard.py --server.address=0.0.0.0
```

Open `http://localhost:8501` in your browser

### 5. Kafka UI

Open `http://localhost:8080` in your browser  
Monitor topic messages, consumer groups, and broker status
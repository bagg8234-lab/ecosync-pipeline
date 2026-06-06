# ⚡ EcoSync — Real-time Renewable Energy Trading Data Pipeline

**🌐 Language:** [한국어](README.md) | [English](README.en.md)

A serverless data pipeline that brokers real-time energy trading between solar energy prosumers and consumers, optimizing energy efficiency through data collection, validation, and matching.

---

## Architecture

```
[Dummy Data Generator]
        ↓
   [Kafka Producer]
        ↓
      [Kafka]          ← Real-time data queue
        ↓
  [Kafka Consumer]
        ↓
   [Validator]         ← Pydantic + Great Expectations
   ↙         ↘
[DLQ]      [PostgreSQL + MinIO]
(Failed)     (Raw + Processed data)
                ↓
         [Matching Engine]  ← Haversine distance-based
                ↓
       [Dynamic Pricing]    ← Weather API + Supply/Demand ratio
                ↓
      [Streamlit Dashboard]
```

---

## Tech Stack

| Category | Local (Docker) | Cloud (Azure) |
| :--- | :--- | :--- |
| Orchestrator | Python Script | Azure Data Factory |
| Compute | Docker (Python 3.12-alpine) | Azure Functions |
| Message Broker | Kafka | Azure Event Hubs |
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

**2. Real-time Trade Matching Engine**
- Distance calculation using Haversine formula
- Nearest supplier priority matching
- Automatic fallback to next candidate when supply is insufficient
- Failed match log stored separately

**3. Dynamic Pricing**
- Korea Meteorological Administration API integration (real-time solar radiation/temperature)
- Price calculation: solar radiation factor × supply/demand ratio × temperature correction
- SMP (System Marginal Price) as baseline

---

## Project Structure

```
ecosync-project/
├── .env.example            # Environment variable template
├── docker-compose.yml      # Infrastructure definition
├── Dockerfile              # App container build
├── requirements.txt        # Python libraries
├── logs/                   # Log files
└── src/
    ├── data_generator.py   # Dummy data generation
    ├── kafka_producer.py   # Kafka publisher
    ├── kafka_consumer.py   # Kafka consumer (for testing)
    ├── validator.py        # Pydantic validation
    ├── ge_validator.py     # Great Expectations validation
    ├── dead_letter_queue.py # DLQ isolation
    ├── minio_client.py     # MinIO storage
    ├── db_client.py        # PostgreSQL UPSERT
    ├── matching_engine.py  # Trade matching
    ├── weather_api.py      # KMA Weather API
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

# ⚡ EcoSync — 실시간 재생에너지 거래 데이터 파이프라인

**🌐 Language:** [한국어](README.md) | [English](README.en.md)

재생에너지(태양광) 프로슈머와 수요처 간의 실시간 에너지 거래를 중개하고,  
데이터를 수집·검증·매칭하여 에너지 효율을 최적화하는 서버리스 데이터 파이프라인입니다.

---

## 아키텍처

```
[더미 데이터 생성기]
        ↓
   [Kafka Producer]
        ↓
      [Kafka]          ← 실시간 데이터 대기줄
        ↓
  [Kafka Consumer]
        ↓
   [Validator]         ← Pydantic + Great Expectations
   ↙         ↘
[DLQ]      [PostgreSQL + MinIO]
(실패)        (원본 + 처리 데이터)
                ↓
         [매칭 엔진]    ← Haversine 거리 기반
                ↓
       [Dynamic Pricing] ← 기상청 API + 수급 비율
                ↓
      [Streamlit 대시보드]
```

---

## 기술 스택

| 분류 | 로컬 (Docker) | 클라우드 (Azure) |
| :--- | :--- | :--- |
| Orchestrator | Python 스크립트 | Azure Data Factory |
| Compute | Docker (Python 3.12-alpine) | Azure Functions |
| Message Broker | Kafka | Azure Event Hubs |
| Storage | MinIO (S3 호환) | ADLS Gen2 |
| Database | PostgreSQL v16-alpine | Azure SQL Database |
| Visualization | Streamlit / Tableau | Power BI |
| IaC | Docker Compose | Terraform |

---

## 주요 기능

**1. 데이터 무결성 검증**
- Pydantic — 타입/범위/필수값 검증 (음수 발전량, null 차단)
- Great Expectations — 통계적 이상치 감지 (전체 분포 검증)
- 검증 실패 데이터 → Dead Letter Queue 격리

**2. 실시간 거래 매칭 엔진**
- Haversine 공식으로 위도/경도 거리 계산
- 가장 가까운 공급자 우선 매칭
- 공급 부족 시 다음 후보로 자동 넘김
- 매칭 실패 로그 분리 적재

**3. Dynamic Pricing**
- 기상청 API 연동 (실시간 일사량/기온 데이터)
- 일사량 계수 × 수급 비율 × 기온 보정으로 가격 산출
- SMP(계통한계가격) 기준선 활용

---

## 폴더 구조

```
ecosync-project/
├── .env.example            # 환경 변수 샘플
├── docker-compose.yml      # 인프라 정의
├── Dockerfile              # 앱 컨테이너 빌드
├── requirements.txt        # 파이썬 라이브러리
├── logs/                   # 로그 파일
└── src/
    ├── data_generator.py   # 더미 데이터 생성
    ├── kafka_producer.py   # Kafka 발행
    ├── kafka_consumer.py   # Kafka 수신 (테스트용)
    ├── validator.py        # Pydantic 검증
    ├── ge_validator.py     # Great Expectations 검증
    ├── dead_letter_queue.py # DLQ 격리
    ├── minio_client.py     # MinIO 적재
    ├── db_client.py        # PostgreSQL UPSERT
    ├── matching_engine.py  # 거래 매칭
    ├── weather_api.py      # 기상청 API
    ├── dynamic_pricing.py  # 가격 산출
    ├── pipeline.py         # End-to-End 파이프라인
    └── dashboard.py        # Streamlit 대시보드
```

---

## 실행 방법

### 1. 환경 변수 설정

```bash
cp .env.example .env
# .env 파일에 API 키 입력
```

### 2. 인프라 실행

```bash
docker-compose up -d
```

### 3. 파이프라인 실행

터미널 1 — 파이프라인:
```bash
docker exec -it ecosync-app python src/pipeline.py
```

터미널 2 — 데이터 발행:
```bash
docker exec -it ecosync-app python src/kafka_producer.py
```

### 4. 대시보드 실행

```bash
docker exec -it ecosync-app streamlit run src/dashboard.py --server.address=0.0.0.0
```

브라우저에서 `http://localhost:8501` 접속


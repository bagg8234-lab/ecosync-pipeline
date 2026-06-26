import json
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from generation_api import get_solar_generation
from data_generator import generate_demand_data

CITY_COORDS = {
    "서울": {"lat": 37.5665, "lon": 126.9780},
    "부산": {"lat": 35.1796, "lon": 129.0756},
    "인천": {"lat": 37.4563, "lon": 126.7052},
    "대구": {"lat": 35.8722, "lon": 128.6025},
    "대전": {"lat": 36.3504, "lon": 127.3845},
}

def create_producer():
    for i in range(5):
        try:
            producer = KafkaProducer(
                bootstrap_servers='kafka:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Kafka 연결 성공!")
            return producer
        except NoBrokersAvailable:
            print(f"Kafka 연결 실패 ({i+1}/5) — 5초 후 재시도...")
            time.sleep(5)
    raise Exception("Kafka 연결 실패 — 컨테이너 상태 확인 필요")

producer = create_producer()

def publish_generation_data():
    data = get_solar_generation()
    if not data:
        print("발전량 API 데이터 없음 — 스킵")
        return
    for record in data:
        formatted = {
            "id": f"kpx-{record['city']}-{record['hour']}",
            "timestamp": f"{record['trade_date']}T{record['hour']:02d}:00:00",
            "city": record['city'],
            "location_lat": CITY_COORDS[record['city']]['lat'],
            "location_lon": CITY_COORDS[record['city']]['lon'],
            "generation_kwh": record['generation_kwh'],
        }
        producer.send('generation', value=formatted)
        print(f"발전량 데이터 게시: {record['city']} | {record['generation_kwh']} kWh")
    producer.flush()

def publish_demand_data(num_records: int = 10):
    data = generate_demand_data(num_records)
    for record in data:
        producer.send('demand', value=record)
        print(f"전력 수요 데이터 게시: {record['city']} | {record['demand_kwh']} kWh")
    producer.flush()

if __name__ == "__main__":
    print("Kafka Producer 시작...")
    while True:
        publish_generation_data()
        publish_demand_data(5)
        print("10초 대기...")
        time.sleep(10)

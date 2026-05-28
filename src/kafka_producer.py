import json
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from data_generator import generate_generation_data, generate_demand_data

# Kafka 연결 (재시도 로직 추가)
def create_producer():
    for i in range(5):  # 최대 5번 재시도
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

def publish_generation_data(num_records: int=10):
    """ 태양광 발전량 데이터를 Kafka 토픽에 게시"""
    data = generate_generation_data(num_records)
    for record in data:
        producer.send('generation', value=record)
        print(f"발전량 데이터 게시: {record['city']} | {record['generation_kwh']} kWh")
    # Kafka는 성능을 위해 데이터를 버퍼에 모았다가 한번에 보냄
    producer.flush()  

def publish_demand_data(num_records: int=10):
    """ 전력 수요 데이터를 Kafka 토픽에 게시"""
    data = generate_demand_data(num_records)
    for record in data:
        producer.send('demand', value=record)
        print(f"전력 수요 데이터 게시: {record['city']} | {record['demand_kwh']} kWh")
    producer.flush()  

if __name__ == "__main__":
    print("Kafka Producer 시작...")
    while True:
        publish_generation_data(5)
        publish_demand_data(5)
        print("10초 대기...")
        time.sleep(10)  # 10초마다 데이터 게시
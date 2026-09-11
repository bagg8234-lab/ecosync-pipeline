"""
테스트 전용: generation도 KPX 실제 API 대신 오늘 날짜 더미 데이터로 발행.
목적: trade_date 그룹화 로직이 "같은 날짜"일 때 정상적으로 매칭되는 것을 확인.

사용법:
  docker exec -it ecosync-app python src/test_same_day_producer.py

주의: 이건 테스트 전용 스크립트입니다. kafka_producer.py(운영용, KPX 실제 API 사용)는
그대로 두고, 이 스크립트로 "같은 날짜 매칭 성공" 케이스만 짧게 검증한 뒤
정리(삭제 또는 보관)하시면 됩니다.
"""
import json
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from data_generator import generate_generation_data, generate_demand_data


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


def publish_same_day_test(num_records: int = 10):
    gen_data = generate_generation_data(num_records)
    for record in gen_data:
        producer.send('generation', value=record)
        print(f"[TEST] 발전량 게시: {record['city']} | {record['generation_kwh']} kWh | {record['timestamp']}")

    dem_data = generate_demand_data(num_records)
    for record in dem_data:
        producer.send('demand', value=record)
        print(f"[TEST] 수요 게시: {record['city']} | {record['demand_kwh']} kWh | {record['timestamp']}")

    producer.flush()


if __name__ == "__main__":
    print("[TEST] 같은 날짜(오늘) generation/demand 발행 시작...")
    publish_same_day_test(10)
    print("[TEST] 발행 완료 — pipeline.py 콘솔에서 매칭 결과를 확인하세요.")
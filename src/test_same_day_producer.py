"""
테스트 전용: generation도 KPX 실제 API 대신 오늘 날짜 더미 데이터로 발행.
목적: trade_date 그룹화 로직이 "같은 날짜"일 때 정상적으로 매칭되는 것을 확인.

사용법:
  docker exec -it ecosync-app python src/test_same_day_producer.py

주의: 이건 테스트 전용 스크립트입니다.
"""

import json
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from data_generator import generate_generation_data, generate_demand_data
 
BOOST_FACTOR = 50  # 발전량을 넉넉하게 키우는 배수 (필요시 조정)
 
 
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
 
 
def publish_clean_match_test(num_records: int = 10):
    gen_data = generate_generation_data(num_records)
    for record in gen_data:
        # 발전량 부족(밤 시간대 등)으로 매칭 실패가 섞이지 않도록 넉넉하게 증폭
        record["generation_kwh"] = round(record["generation_kwh"] * BOOST_FACTOR + 200, 2)
        producer.send('generation', value=record)
        print(f"[TEST] 발전량 게시: {record['city']} | {record['generation_kwh']} kWh | {record['timestamp']}")
 
    dem_data = generate_demand_data(num_records)
    for record in dem_data:
        producer.send('demand', value=record)
        print(f"[TEST] 수요 게시: {record['city']} | {record['demand_kwh']} kWh | {record['timestamp']}")
 
    producer.flush()
 
 
if __name__ == "__main__":
    print("[TEST] 공급 넉넉한 같은 날짜(오늘) generation/demand 발행 시작...")
    publish_clean_match_test(10)
    print("[TEST] 발행 완료 — pipeline.py 콘솔에서 매칭 결과를 확인하세요.")
 
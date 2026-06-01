import json
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import time

def create_dlq_producer():
    """Dead Letter Queue Producer 생성 """
    for i in range(5):
        try:
            producer = KafkaProducer(
                bootstrap_servers='kafka:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("DLQ Producer 연결 성공")
            return producer
        except NoBrokersAvailable:
            print(f"연결 실패 {i+1}/5 - 5초 후 재시도")
            time.sleep(5)
    raise Exception("DLQ Producer 연결 실패")

dlq_producer = create_dlq_producer()

def send_to_dlq(data: dict, reason: str, data_type: str):
    """
    검증 실패한 데이터를 Dead Letter Queue로 격리
    - data: 검증 실패한 데이터
    - reason: 실패 사유
    - data_type: 데이터 유형 (예: 'generation', 'demand')
    """
    dlq_message = {
        "original_data": data,
        "reason": reason,
        "data_type": data_type,
        "failed_at": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    # dead-letter 토픽으로 발행
    dlq_producer.send('dead-letter', value=dlq_message)
    dlq_producer.flush()  # 즉시 전송
    print(f"DLQ 격리: {data_type} | 사유: {reason}")

if __name__ == "__main__":
    from validator import validate_generation
    from data_generator import generate_generation_data

    print("DLQ 테스트")

    data_list = generate_generation_data(5)

    # 의도적으로 오염
    data_list[0]['generation_kwh'] = -99.0  # 음수 발전량
    data_list[1]['city'] = None  # 도시명 누락

    for data in data_list:
        is_valid, error = validate_generation(data)
        if is_valid:
            print(f"통과: {data['city']} | {data['generation_kwh']} kWh")
        else:
            send_to_dlq(data, error, "generation")
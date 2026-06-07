import json
import time
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from validator import validate_generation, validate_demand

def create_consumer():
    for i in range(5):
        try:
            consumer = KafkaConsumer(
                'dead-letter',
                bootstrap_servers='kafka:9092',
                value_deserializer=lambda v:json.loads(v.decode('utf-8')),
                auto_offset_reset='earliest',
                group_id='dlq-reprocessor'
            )
            print("DLP Consumer 연결 성공")
            return consumer
        except NoBrokersAvailable:
            print(f"연결 실패 ({i+1}/5) - 5초 후 재시도...")
            time.sleep(5)
        raise Exception("DLQ Consumer 연결 실패")


def create_producer():
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
            print(f"연결 실패 {i+1}/5 - 5초 후 재시도...")
            time.sleep(5)
    raise Exception("DLQ Producer 연결 실패")

def reprocess_dlq():
    """
    DLQ에 쌓인 실패 데이터 재처리
    - 재검증 통과 -> 원래 토픽으로 재발행
    - 재검증 실패 -> 스킵(로그인 출력)
    """
    consumer = create_consumer()
    producer = create_producer()

    print("\n DLQ 재처리 시작 \n")

    success = 0
    fail = 0
    
    for message in consumer:
        dlq_message = message.value
        original_data = dlq_message.get('original_data')
        data_type = dlq_message.get('data_type')
        reason = dlq_message.get('reason')
        failed_at = dlq_message.get('failed_at')

        print(f"재처리 시도: {data_type} | 원래 실패 사유: {reason} | 실패 시각: {failed_at}")

        # 데이터 타입에 따라 재검증
        if data_type == 'generation':
            is_valid, error = validate_generation(original_data)
        elif data_type == 'demand':
            is_valid, error = validate_demand(original_data)
        else:
            print(f"알 수 없는 데이터 타입: {data_type} - 스킵")
            continue

        if is_valid:
            # 재검증 통과 -> 원래 토픽으로 재발행
            producer.send(data_type, value=original_data)
            producer.flush()
            print(f"✅ 재처리 성공 -> {data_type} 토픽 재발행")
            success +=1
        else:
            # 재검증 실패 -> 스킵
            print(f"❌ 재처리 실패 - 여전히 유효하지 않음: {error}")
            fail+=1

    print(f"\n 💡 DLQ 재처리 완료")

if __name__ == "__main__":
    reprocess_dlq()

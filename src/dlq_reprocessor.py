import json
import time
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError
from validator import validate_generation, validate_demand
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type, before_sleep_log
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_consumer():
    for i in range(5):
        try:
            consumer = KafkaConsumer(
                'dead-letter',
                bootstrap_servers='kafka:9092',
                value_deserializer=lambda v:json.loads(v.decode('utf-8')),
                auto_offset_reset='earliest',
                group_id=f'dlq-reprocessor-{int(time.time())}',
                consumer_timeout_ms=10000  # 10초 동안 메시지 없으면 종료
            )
            print("DLQ Consumer 연결 성공")
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

@retry(
    wait=wait_exponential(multiplier=1, min=1, max=30),  # 1초 → 2초 → 4초 → 8초 → 16초 (최대 30초)
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type(KafkaError),
    before_sleep=before_sleep_log(logger, logging.WARNING)
)
def republish_with_backoff(producer, data_type, original_data):
    """재검증 통과 데이터를 원래 토픽으로 재발행. 실패 시 지수 백오프로 최대 5회 재시도"""
    producer.send(data_type, value=original_data)
    producer.flush()

def reprocess_dlq():
    """
    DLQ에 쌓인 실패 데이터 재처리
    - error_type == 'data_error'   : 데이터 자체 오류 → 재처리해도 실패하므로 스킵, 로그만 출력
    - error_type == 'system_error' : 일시적 장애 → 재검증 후 통과 시 원래 토픽으로 재발행
    - error_type 없음 (구버전 메시지): 기존 방식대로 재검증 후 처리
    """
    consumer = create_consumer()
    producer = create_producer()

    print("\n DLQ 재처리 시작 \n")

    success = 0
    skipped = 0
    fail = 0
    
    for message in consumer:
        dlq_message = message.value
        original_data = dlq_message.get('original_data')
        data_type = dlq_message.get('data_type')
        reason = dlq_message.get('reason')
        failed_at = dlq_message.get('failed_at')
        error_type = dlq_message.get('error_type', 'unknown')

        print(f"재처리 시도: {data_type} | error_type: {error_type} | 원래 실패 사유: {reason} | 실패 시각: {failed_at}")

        # data_error는 재처리해도 동일하게 실패 -> 스킵
        if error_type == 'data_error':
            print(f"⏭️  스킵 [data_error]: 데이터 자체 오류는 수동 확인 필요 — {reason}")
            skipped +=1
            continue

        # 데이터 타입에 따라 재검증
        if data_type == 'generation':
            is_valid, error = validate_generation(original_data)
        elif data_type == 'demand':
            is_valid, error = validate_demand(original_data)
        else:
            print(f"알 수 없는 데이터 타입: {data_type} - 스킵")
            skipped += 1
            continue

        if is_valid:
            try:
                republish_with_backoff(producer, data_type, original_data)
                print(f"✅ 재처리 성공 -> {data_type} 토픽 재발행")
                success += 1
            except KafkaError as e:
                # 5회 재시도(지수 백오프)까지 모두 실패 -> 다음 cron 사이클로 넘김
                print(f"❌ 재발행 최종 실패 (5회 재시도 후): {e}")
                fail += 1
        else:
            # 재검증 실패 -> 스킵
            print(f"❌ 재처리 실패 - 여전히 유효하지 않음: {error}")
            fail+=1

    print(f"\n 💡 DLQ 재처리 완료 | 성공: {success} | 스킵: {skipped} | 실패: {fail}")

if __name__ == "__main__":
    reprocess_dlq()

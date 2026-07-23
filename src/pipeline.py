import json
import time
import psycopg2
from botocore.exceptions import ConnectionError as MinioConnectionError
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from validator import validate_generation, validate_demand
from dead_letter_queue import send_to_dlq
from ge_validator import validate_generation_stats, validate_demand_stats
from matching_engine import match_generation_to_demand, save_trades
from db_client import upsert_generation, upsert_demand, create_table
from minio_client import create_minio_client, create_bucket, upload_raw_data


def create_consumer(topics: list[str]):
    for i in range(5):
        try:
            consumer = KafkaConsumer(
                *topics,
                bootstrap_servers='kafka:9092',
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset='latest',
                group_id='ecosync-pipeline-v2'
            )
            print(f"파이프라인 Consumer 연결 성공!")
            return consumer
        except NoBrokersAvailable:
            print(f"연결 실패 ({i+1}/5) — 5초 후 재시도...")
            time.sleep(5)
    raise Exception("Consumer 연결 실패")

def process_generation(data: dict, gen_pydantic_buffer: list):
    is_valid, error = validate_generation(data)
    if not is_valid:
        send_to_dlq(data, error, 'generation', error_type="data_error")
        print(f"❌ 발전량 DLQ [data_error]: {data.get('city')} | {error[:50]}")
        return
    gen_pydantic_buffer.append(data)
    print(f"✅ Pydantic 통과 (GE 대기): {data['city']} | {data['generation_kwh']} kWh")


def process_demand(data: dict, dem_pydantic_buffer: list):
    is_valid, error = validate_demand(data)
    if not is_valid:
        send_to_dlq(data, error, 'demand', error_type="data_error")
        print(f"❌ 소비량 DLQ [data_error]: {data.get('city')} | {error[:50]}")
        return
    dem_pydantic_buffer.append(data)
    print(f"✅ Pydantic 통과 (GE 대기): {data['city']} | {data['demand_kwh']} kWh")

# GE 배치 검증 후 통과분만 DB/MinIO 저장
def process_ge_batch(pydantic_buffer: list, data_type: str, minio_client, target_buffer: list):
    if not pydantic_buffer:
        return

    try:
        if data_type == 'generation':
            ge_success, ge_failures, ge_results = validate_generation_stats(pydantic_buffer)
        else:
            ge_success, ge_failures, ge_results = validate_demand_stats(pydantic_buffer)
    except Exception as e:
        # GE 실행 자체가 실패(환경/리소스 문제 등) → 일시적 문제이므로 재처리 시 복구 가능
        for record in pydantic_buffer:
            send_to_dlq(record, f"GE 검증 실행 실패: {str(e)}", data_type, error_type="system_error")
        print(f"❌ {data_type} DLQ [system_error]: GE 실행 오류 | {str(e)[:50]}")
        return

    if ge_success:
        clean_records = pydantic_buffer
        rejected = []
    else:
        # GE가 잡아낸 이상치 값들
        unexpected_values = set()
        for r in ge_results.results:                                          
            if not r.success:
                unexpected_values.update(r.result.get("partial_unexpected_list", []))

        clean_records, rejected = [], []
        value_key = 'generation_kwh' if data_type == 'generation' else 'demand_kwh'
        for record in pydantic_buffer:
            if record.get(value_key) in unexpected_values or record.get('city') in unexpected_values:
                rejected.append(record)
            else:
                clean_records.append(record)

    for record in rejected:
        send_to_dlq(record, f"GE 통계 이상치 감지", data_type, error_type="data_error")
        print(f"❌ {data_type} DLQ [data_error, GE]: {record.get('city')}")

    for record in clean_records:
        try:
            if data_type == 'generation':
                upsert_generation(record)
            else:
                upsert_demand(record)
        except psycopg2.OperationalError as e:
            # 실제 DB 연결 장애 → 일시적 문제이므로 재처리 시 복구 가능
            send_to_dlq(record, f"DB 연결 실패: {str(e)}", data_type, error_type="system_error")
            print(f"❌ {data_type} DLQ [system_error]: DB 연결 오류 | {str(e)[:50]}")
            continue
        except Exception as e:
            # 제약 조건 위반(IntegrityError) 등 데이터 자체 문제 → 재처리해도 동일하게 실패
            send_to_dlq(record, f"DB 저장 실패: {str(e)}", data_type, error_type="data_error")
            print(f"❌ {data_type} DLQ [data_error]: DB 오류 | {str(e)[:50]}")
            continue
        try:
            upload_raw_data(minio_client, [record], data_type)
        except MinioConnectionError as e:
            # 실제 MinIO 연결/네트워크 장애 → 일시적 문제이므로 재처리 시 복구 가능
            send_to_dlq(record, f"MinIO 연결 실패: {str(e)}", data_type, error_type="system_error")
            print(f"❌ {data_type} DLQ [system_error]: MinIO 연결 오류 | {str(e)[:50]}")
            continue
        except Exception as e:
            # 버킷/권한 등 요청 자체가 거부된 경우 → 재처리해도 동일하게 실패
            send_to_dlq(record, f"MinIO 저장 실패: {str(e)}", data_type, error_type="data_error")
            print(f"❌ {data_type} DLQ [data_error]: MinIO 오류 | {str(e)[:50]}")
            continue
        target_buffer.append(record)
        print(f"✅ {data_type} 최종 저장: {record.get('city')}")

def run_pipeline():
    print("⚡EcoSync 파이프라인 시작 \n")
    create_table()
    minio_client = create_minio_client()
    create_bucket(minio_client, 'ecosync-raw')
    consumer = create_consumer(['generation', 'demand'])

    gen_pydantic_buffer, dem_pydantic_buffer = [], [] 
    gen_buffer, dem_buffer = [], []  # GE까지 통과 완료 → 매칭 엔진 대상

    GE_BATCH_SIZE = 10
    BATCH_SIZE = 10
    print(f"토픽 구독 중: generation, demand")
    print(f"배치 사이즈: {BATCH_SIZE}개\n")
    for message in consumer:
        topic = message.topic
        data = message.value
        if topic == 'generation':
            process_generation(data, gen_pydantic_buffer)
        elif topic == 'demand':
            process_demand(data, dem_pydantic_buffer)   
            
        # GE 배치 크기만큼 쌓이면 통계 검증 실행
        if len(gen_pydantic_buffer) >= GE_BATCH_SIZE:
            process_ge_batch(gen_pydantic_buffer, 'generation', minio_client, gen_buffer)
            gen_pydantic_buffer.clear()

        if len(dem_pydantic_buffer) >= GE_BATCH_SIZE:
            process_ge_batch(dem_pydantic_buffer, 'demand', minio_client, dem_buffer)
            dem_pydantic_buffer.clear()

        if len(gen_buffer) >= BATCH_SIZE and len(dem_buffer) >= BATCH_SIZE:
            print(f"\n--- 매칭 엔진 실행 ({BATCH_SIZE}개 배치) ---")
            trades, unmatched = match_generation_to_demand(gen_buffer, dem_buffer)
            save_trades(trades, unmatched)
            print(f"거래 체결: {len(trades)}건 | 실패: {len(unmatched)}건\n")
            gen_buffer.clear()
            dem_buffer.clear()

if __name__ == "__main__":
    run_pipeline()

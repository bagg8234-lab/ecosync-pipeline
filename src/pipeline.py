import json
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from validator import validate_generation, validate_demand
from dead_letter_queue import send_to_dlq
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

def process_generation(data: dict, minio_client, gen_buffer: list):
    is_valid, error = validate_generation(data)
    if not is_valid:
        send_to_dlq(data, error, 'generation', error_type="data_error")
        print(f"❌ 발전량 DLQ [data_error]: {data.get('city')} | {error[:50]}")
        return
    try:
        upsert_generation(data)
    except Exception as e:
        send_to_dlq(data, f"DB 저장 실패: {str(e)}", 'generation', error_type="system_error")
        print(f"❌ 발전량 DLQ [system_error]: DB 오류 | {str(e)[:50]}")
        return
    try:
        upload_raw_data(minio_client, [data], 'generation')
    except Exception as e:
        send_to_dlq(data, f"MinIO 저장 실패: {str(e)}", 'generation', error_type="system_error")
        print(f"❌ 발전량 DLQ [system_error]: MinIO 오류 | {str(e)[:50]}")
        return
    gen_buffer.append(data)
    print(f"✅ 발전량 통과: {data['city']} | {data['generation_kwh']} kWh")

def process_demand(data: dict, minio_client, dem_buffer: list):
    is_valid, error = validate_demand(data)
    if not is_valid:
        send_to_dlq(data, error, 'demand', error_type="data_error")
        print(f"❌ 소비량 DLQ [data_error]: {data.get('city')} | {error[:50]}")
        return
    try:
        upsert_demand(data)
    except Exception as e:
        send_to_dlq(data, f"DB 저장 실패: {str(e)}", 'demand', error_type="system_error")
        print(f"❌ 소비량 DLQ [system_error]: DB 오류 | {str(e)[:50]}")
        return
    try:
        upload_raw_data(minio_client, [data], 'demand')
    except Exception as e:
        send_to_dlq(data, f"MinIO 저장 실패: {str(e)}", 'demand', error_type="system_error")
        print(f"❌ 소비량 DLQ [system_error]: MinIO 오류 | {str(e)[:50]}")
        return
    dem_buffer.append(data)
    print(f"✅ 소비량 통과: {data['city']} | {data['demand_kwh']} kWh")

def run_pipeline():
    print("⚡EcoSync 파이프라인 시작 \n")
    create_table()
    minio_client = create_minio_client()
    create_bucket(minio_client, 'ecosync-raw')
    consumer = create_consumer(['generation', 'demand'])
    gen_buffer = []
    dem_buffer = []
    BATCH_SIZE = 10
    print(f"토픽 구독 중: generation, demand")
    print(f"배치 사이즈: {BATCH_SIZE}개\n")
    for message in consumer:
        topic = message.topic
        data = message.value
        if topic == 'generation':
            process_generation(data, minio_client, gen_buffer)
        elif topic == 'demand':
            process_demand(data, minio_client, dem_buffer)
        if len(gen_buffer) >= BATCH_SIZE and len(dem_buffer) >= BATCH_SIZE:
            print(f"\n--- 매칭 엔진 실행 ({BATCH_SIZE}개 배치) ---")
            trades, unmatched = match_generation_to_demand(gen_buffer, dem_buffer)
            save_trades(trades, unmatched)
            print(f"거래 체결: {len(trades)}건 | 실패: {len(unmatched)}건\n")
            gen_buffer.clear()
            dem_buffer.clear()

if __name__ == "__main__":
    run_pipeline()

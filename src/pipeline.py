import json
import time
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from validator import validate_generation, validate_demand
from dead_letter_queue import send_to_dlq
from matching_engine import match_generation_to_demand, save_trades
from db_client import upsert_generation, upsert_demand, create_table
from minio_client import create_storage_client, create_bucket, upload_raw_data

load_dotenv('C:/TOY/ecosync-project/.env')

def create_consumer(topics: list[str]):
    """Kafka Consumer 생성 (재시도 로직 포함)"""
    for i in range(5):
        try:
            consumer = KafkaConsumer(
                *topics,
                bootstrap_servers=os.getenv('EVENT_HUBS_NAMESPACE') + '.servicebus.windows.net:9093',
                security_protocol='SASL_SSL',
                sasl_mechanism='PLAIN',
                sasl_plain_username='$ConnectionString',
                sasl_plain_password=os.getenv('EVENT_HUBS_CONNECTION_STRING'),
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
    """발전량 데이터 처리: 검증 -> DB/MinIO 저장 -> 버퍼 추가"""
    is_valid, error = validate_generation(data)
    if not is_valid:
        send_to_dlq(data, error, 'generation', error_type="data_error")
        print(f"❌ 발전량 DLQ [data_error]: {data.get('city')} | {error[:50]}")
        return
    
    # DB 저장 - 실패 시 system_error로 DLQ
    try:
        upsert_generation(data)
    except Exception as e:
        send_to_dlq(data, f"DB 저장 실패: {str(e)}", 'generation', error_type="system_error")
        print(f"❌ 발전량 DLQ [system_error]: DB 오류 | {str(e)[:50]}")
        return
    
    # MinIO 저장 - 실패 시 system_error로 DLQ
    try:
        upload_raw_data(minio_client, [data], 'generation')
    except Exception as e:
        send_to_dlq(data, f"MinIO 저장 실패: {str(e)}", 'generation', error_type="system_error")
        print(f"❌ 발전량 DLQ [system_error]: MinIO 오류 | {str(e)[:50]}")
        return
    
    gen_buffer.append(data)
    print(f"✅ 발전량 통과: {data['city']} | {data['generation_kwh']} kWh" )

def process_demand(data: dict, minio_client, dem_buffer: list):
    """소비량 데이터 처리: 검증 → DB/MinIO 저장 → 버퍼 추가"""
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
    """
    Kafka Consumer → Validator → 매칭 엔진 전체 파이프라인
    """
    print("⚡EcoSync 파이프라인 시작 \n")

    # 초기화
    create_table()
    minio_client = create_storage_client()
    create_bucket(minio_client, 'ecosync-raw')

    # Kafka Consumer 생성 (generation + demand 토픽 구독)
    consumer = create_consumer(['generation', 'demand'])

    gen_buffer = []   # 발전량 데이터 버퍼
    dem_buffer = []   # 소비량 데이터 버퍼
    BATCH_SIZE = 10   # 10개 모이면 매칭 실행

    print(f"토픽 구독 중: generation, demand")
    print(f"배치 사이즈: {BATCH_SIZE}개\n")

    for message in consumer:
        topic = message.topic
        data = message.value

        if topic == 'generation':
            process_generation(data, minio_client, gen_buffer)
        elif topic == 'demand':
            process_demand(data, minio_client, dem_buffer)

        # 버퍼에 10개씩 쌓이면 매칭 실행
        if len(gen_buffer) >= BATCH_SIZE and len(dem_buffer) >= BATCH_SIZE:
            print(f"\n--- 매칭 엔진 실행 ({BATCH_SIZE}개 배치) ---")
            trades, unmatched = match_generation_to_demand(gen_buffer, dem_buffer)
            save_trades(trades, unmatched)
            print(f"거래 체결: {len(trades)}건 | 실패: {len(unmatched)}건\n")

            # 버퍼 초기화
            gen_buffer.clear()
            dem_buffer.clear()


if __name__ == "__main__":
    run_pipeline()
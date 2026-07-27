import boto3
import json
import logging
from datetime import datetime
from botocore.exceptions import ClientError
from botocore.config import Config
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.DEBUG)
logging.getLogger('botocore').setLevel(logging.DEBUG) # 재시도 로그 시각화

def create_minio_client(max_pool_connections: int = 10, max_attempts: int = 5):
    """MinIO 클라이언트 생성"""
    return boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin',
        config=Config(
            max_pool_connections=max_pool_connections,
            retries={
                'max_attempts': max_attempts,
                'mode': 'standard'
            },
            connect_timeout=3, # 연결 시도 3초 넘으면 타임아웃
            read_timeout=3)
    )

def create_bucket(client, bucket_name: str):
    """버킷 없으면 생성"""
    try:
        client.head_bucket(Bucket=bucket_name)
        print(f"버킷 '{bucket_name}' 이미 존재")
    except ClientError:
        client.create_bucket(Bucket=bucket_name)
        print(f"버킷 '{bucket_name}' 생성 완료")

def upload_raw_data(client, data: list[dict], data_type: str):
    """
    원본 데이터 MinIO에 업로드
    - data_type: 'generation' 또는 'demand'
    - 날짜/시간 기준으로 폴더 구조 생성
    """
    bucket_name = 'ecosync-raw'
    now = datetime.now()
    file_path = f"{data_type}/{now.year}/{now.month:02d}/{now.day:02d}/{now.hour:02d}-{now.minute:02d}-{now.second:02d}.json"
    client.put_object(
        Bucket=bucket_name,
        Key=file_path,
        Body=json.dumps(data).encode('utf-8'),
        ContentType='application/json'
    )
    print(f"'{data_type}' 데이터가 '{file_path}' 경로로 업로드되었습니다.")

def test_pool_exhaustion():
    client = create_minio_client(max_pool_connections=2)  # 풀 크기 작게
    create_bucket(client, 'ecosync-raw')

    from data_generator import generate_generation_data

    def upload_task(i):
        data = generate_generation_data(1)
        upload_raw_data(client, data, 'generation')
        print(f"업로드 {i} 완료")

    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(upload_task, range(10))

def test_retry_on_connection_failure():
    """MinIO 연결 장애 시 자동 재시도 동작 확인"""
    client = create_minio_client(max_attempts=3)
    from data_generator import generate_generation_data

    data = generate_generation_data(1)
    print("업로드 시도 중...")
    try:
        upload_raw_data(client, data, 'generation')
        print("✅ 업로드 성공")
    except Exception as e:
        print(f"❌ 재시도 {3}번 다 실패: {e}")

if __name__ == "__main__":
    # MinIO 연결 테스트 및 데이터 업로드
    # from data_generator import generate_generation_data, generate_demand_data
    # client = create_minio_client()
    # create_bucket(client, 'ecosync-raw')
    # generation_data = generate_generation_data(5)
    # upload_raw_data(client, generation_data, 'generation')
    # demand_data = generate_demand_data(5)
    # upload_raw_data(client, demand_data, 'demand')
    # print("\nMinIO 콘솔에서 확인: http://localhost:9001")

    # 커넥션 풀 고갈 재현 테스트
    #test_pool_exhaustion()

    # 재시도 동작 확인 테스트
    test_retry_on_connection_failure()
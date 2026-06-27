import boto3
import json
from datetime import datetime
from botocore.exceptions import ClientError

def create_minio_client():
    """MinIO 클라이언트 생성"""
    return boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin',
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

if __name__ == "__main__":
    from data_generator import generate_generation_data, generate_demand_data
    client = create_minio_client()
    create_bucket(client, 'ecosync-raw')
    generation_data = generate_generation_data(5)
    upload_raw_data(client, generation_data, 'generation')
    demand_data = generate_demand_data(5)
    upload_raw_data(client, demand_data, 'demand')
    print("\nMinIO 콘솔에서 확인: http://localhost:9001")
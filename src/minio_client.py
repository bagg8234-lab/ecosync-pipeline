import os
import json
from datetime import datetime
from azure.storage.blob import BlobServiceClient

def create_storage_client():
    """MinIO 클라이언트 생성"""
    return BlobServiceClient.from_connection_string(
        os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    )

def create_bucket(client, container_name: str):
    """버킷 없으면 생성"""
    try:
        client.get_container_client(container_name).get_container_properties()
        print(f"컨테이너 '{container_name} 이미 존재")
    except Exception:
        client.create_container(container_name)
        print(f"컨테이너 '{container_name}' 생성 완료")

def upload_raw_data(client, data: list[dict], data_type:str):
    """
    원본 데이터 ADLS Gen2에 업로드
    - data_type: 'generation' 또는 'demand'
    - 날짜/시간 기준으로 폴더 구조 생성
    """
    container_name = 'ecosync-raw'
    now = datetime.now()

    # 파일 경로 : generation/2026/05/31/15-30-00.json
    file_path = f"{data_type}/{now.year}/{now.month:02d}/{now.day:02d}/{now.hour:02d}-{now.minute:02d}-{now.second:02d}.json"

    blob_client = client.get_blob_client(container=container_name, blob=file_path)
    blob_client.upload_blob(
        json.dumps(data).encode('utf-8'),
        overwrite=True,
        content_settings=None
    )
    print(f"'{data_type}' 데이터가 '{file_path}' 경로로 업로드되었습니다.")

if __name__ == "__main__":
    from data_generator import generate_generation_data, generate_demand_data

    client = create_storage_client()
    create_bucket(client, 'ecosync-raw')

    # 발전량 데이터 저장
    generation_data = generate_generation_data(5)
    upload_raw_data(client, generation_data, 'generation')

    # 소비량 데이터 저장
    demand_data = generate_demand_data(5)
    upload_raw_data(client, demand_data, 'demand')

    print("\nAzure Portal에서 확인: https://portal.azure.com")

import numpy as np
from faker import Faker
from datetime import datetime
import uuid

# 한국어 설정
# 나라마다 전화번호, 주소, 이름 등의 형식이 다르기 때문에 Faker는 다양한 로케일을 지원합니다.
fake = Faker('ko_KR')

LOCATIONS = [
    {"city": "서울", "lat": 37.5665, "lon": 126.9780},
    {"city": "부산", "lat": 35.1796, "lon": 129.0756},
    {"city": "인천", "lat": 37.4563, "lon": 126.7052},
    {"city": "대구", "lat": 35.8722, "lon": 128.6025},
    {"city": "대전", "lat": 36.3504, "lon": 127.3845}
]


def generate_generation_data(num_records: int=10) -> list[dict]: # 기본 값 10개로 설정, 필요에 따라 조정 가능
    """
    태양광 발전량 더미 데이터 생성
    - 시간대별 패턴 방영(낮엔 많이, 밤엔 0)
    """
    records = []
    now = datetime.now()
    hour = now.hour

    # 시간대별 패턴 설정
    if 6 <= hour < 18:  # 낮 시간
        solar_factor = 1 - abs(12 - hour) / 6  # 정오에 최대 발전량, 아침과 저녁에는 감소
    else:  # 밤 시간
        solar_factor = 0  # 밤에는 발전량이 없음

    for _ in range(num_records):
        # LOCATIONS 리스트에서 무작위로 하나의 위치를 선택하여 데이터 생성
        location = fake.random_element(LOCATIONS)
        records.append({
            # 절대 겹치지 않는 유일한 ID를 생성하기 위해 uuid 사용
            "id" : str(uuid.uuid4()),
            # 로컬 환경이 한국시간대로 설정되어있지만 나중에 클라우드로 옮길 때 시간대 문제가 발생할 수 있으므로 ISO 8601 형식으로 저장
            # 전 세계 모든 시스템(DB, 분석 툴, 클라우드 등)이 서로 시간 데이터를 주고받을 때 싸우지 않으려고 약속한 '표준 글쓰기 방식'
            "timestamp": now.isoformat(),
            "city": location["city"],
            "location_lat": location["lat"] + np.random.uniform(-0.05, 0.05),  # 약간의 위치 변동
            "location_lon": location["lon"] + np.random.uniform(-0.05, 0.05),  # 약간의 위치 변동
            "generation_kwh" : round(np.random.uniform(50, 500) * solar_factor, 2),  # 시간대별 패턴 반영
        })
    return records

def generate_demand_data(num_records: int=10) -> list[dict]:
    """
    전력 수요 더미 데이터 생성
    - 시간대별 패턴 방영(출퇴근 시간에 수요 증가)
    """
    records = []
    now = datetime.now()
    hour = now.hour

    # 시간대별 패턴 설정
    if 8 <= hour < 10 or 17 <= hour < 22:  # 출퇴근 시간
        demand_factor = 1.5  # 수요 증가
    elif 0 <= hour < 4:  # t새벽
        demand_factor = 0.5  # 수요 감소
    else:  # 그 외 시간
        demand_factor = 1.0  # 기본 수요

    for _ in range(num_records):
        location = fake.random_element(LOCATIONS)
        records.append({
            "id" : str(uuid.uuid4()),
            "timestamp": now.isoformat(),
            "city": location["city"],
            "location_lat": location["lat"] + np.random.uniform(-0.05, 0.05),  # 약간의 위치 변동
            "location_lon": location["lon"] + np.random.uniform(-0.05, 0.05),  # 약간의 위치 변동
            "demand_kwh" : round(np.random.uniform(100, 400) * demand_factor, 2),  # 시간대별 패턴 반영
        })
    return records

# 테스트 실행
if __name__ == "__main__":
    print("태양광 발전량 데이터 샘플:")
    for data in generate_generation_data(5):
        print(data)
    
    print("\n전력 수요 데이터 샘플:")
    for data in generate_demand_data(5):
        print(data)

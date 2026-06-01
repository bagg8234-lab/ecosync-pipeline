from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import Optional

# 발전량 데이터 스키마
class GenerationData(BaseModel):
    id : str
    timestamp: str
    city: str
    location_lat: float = Field(ge=-90, le=90) # 위도 범위 -90 ~ 90
    location_lon: float = Field(ge=-180, le=180) # 경도 범위 -180 ~ 180
    generation_kwh: float = Field(ge=0) # 음수 발전량 차단

    @field_validator('generation_kwh')
    def check_generation_range(cls, v):
        if v > 10000: # 현실적인 발전량 범위 설정
            raise ValueError('발전량은 0에서 10,000 kWh 사이여야 합니다.')
        return v
    
# 소비량 데이터 스키마
class DemandData(BaseModel):
    id : str
    timestamp: str
    city: str
    location_lat: float = Field(ge=-90, le=90) # 위도 범위 -90 ~ 90
    location_lon: float = Field(ge=-180, le=180) # 경도 범위 -180 ~ 180
    demand_kwh: float = Field(ge=0) # 음수 소비량 차단

    @field_validator('demand_kwh')
    def check_demand_range(cls, v):
        if v > 10000: # 현실적인 소비량 범위 설정
            raise ValueError('소비량은 0에서 10,000 kWh 사이여야 합니다.')
        return v
    
def validate_generation(data: dict) -> tuple[bool, Optional[str]]:
    """발전량 데이터 검증, 성공: (True, None), 실패: (False, 에러 메시지)"""
    try:
        GenerationData(**data)
        return True, None
    except Exception as e:
        return False, str(e)

def validate_demand(data: dict) -> tuple[bool, Optional[str]]:
    """소비량 데이터 검증, 성공: (True, None), 실패: (False, 에러 메시지)"""
    try:
        DemandData(**data)
        return True, None
    except Exception as e:
        return False, str(e)

if __name__ == "__main__":
    # 정상 데이터
    valid_data = {
        "id" : "test-001",
        "timestamp" : "2026-05-31T15:30:00",
        "city" : "서울",
        "location_lat" : 37.5665,
        "location_lon" : 126.9780,
        "generation_kwh" : 245.3
    }

    # 오염된 데이터
    invalid_data = {
        "id" : "test-002",
        "timestamp" : "2026-05-31T15:30:00",
        "city" : "서울",
        "location_lat" : -100.1796,
        "location_lon" : 129.9780,
        "generation_kwh" : -50.0 # 음수 발전량
    }

    print("Pydantic 검증 테스트")
    is_valid, error = validate_generation(valid_data)
    print(f"정상 데이터: {'통과' if is_valid else f'실패 - {error}'}")

    is_valid, error = validate_generation(invalid_data)
    print(f"오염된 데이터: {'통과' if is_valid else f'실패 - {error}'}")
"""
dynamic_pricing.py 에 추가할 함수

기존 get_city_price()는 도시마다 get_solar_radiation(), get_smp()를
각각 호출해서, 5개 도시를 순회하면 외부 API가 최대 10번 호출된다.
(각 호출이 timeout=10초라 최악의 경우 대시보드 로딩이 크게 지연됨)

get_all_city_prices()는:
  - get_smp()를 1번만 호출해서 base_price를 구하고
  - get_solar_radiation()을 인자 없이 1번만 호출해서 5개 도시 기상 데이터를 한번에 받고
  - 각 도시는 이미 받아온 데이터에서 계산만 수행 (추가 API 호출 없음)
"""

from weather_api import get_solar_radiation
from smp_api import get_smp
from dynamic_pricing import calculate_price  # 기존 calculate_price 재사용


def get_all_city_prices(city_gen_dem: list[dict]) -> list[dict]:
    """
    city_gen_dem: [{"city": "서울", "generation_kwh": ..., "demand_kwh": ...}, ...]
    반환: 도시별 가격 정보 리스트 (get_city_price와 동일한 딕셔너리 형태)
    """
    # 1) SMP는 전국 공통 값이므로 한 번만 조회
    smp_data = get_smp()
    base_price = smp_data['smp']

    # 2) 기상청은 인자 없이 호출하면 등록된 5개 지점을 한 번에 반환
    weather_list = get_solar_radiation()  # city 인자 생략 → 전체 도시 일괄 조회
    weather_by_city = {w['city']: w for w in weather_list}

    results = []
    for item in city_gen_dem:
        city = item['city']
        weather = weather_by_city.get(city)

        if weather is None:
            results.append({
                "city": city,
                "price": base_price,
                "reason": "기상 데이터 없음 - SMP 기본 가격 적용",
            })
            continue

        price = calculate_price(
            generation_kwh=item['generation_kwh'],
            demand_kwh=item['demand_kwh'],
            solar_radiation=weather['solar_radiation_si'],
            temperature=weather['temperature_ta'],
            base_price=base_price,
        )
        results.append({
            "city": city,
            "price": price,
            "smp": base_price,
            "solar_radiation": weather['solar_radiation_si'],
            "temperature": weather['temperature_ta'],
            "generation_kwh": item['generation_kwh'],
            "demand_kwh": item['demand_kwh'],
            "observed_at": weather['observed_at'],
        })

    return results
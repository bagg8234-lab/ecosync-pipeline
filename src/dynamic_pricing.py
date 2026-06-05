from weather_api import get_solar_radiation

# 기준 가격 (SMP 계통한계가격 기준, 원/kWh)
BASE_PRICE = 150.0

# 태양광 패널 최적 온도 (25°C 초과 시 효율 저하)
OPTIMAL_TEMP = 25.0

def calculate_price(
    generation_kwh: float,
    demand_kwh: float,
    solar_radiation: float,
    temperature: float,
) -> float:
  """
  Dynamic Pricing 알고리즘
  - 일사량 높음 -> 공급 많음 -> 가격 낮춤
  - 수요 > 공급 -> 가격 높임
  - 기온 25°C 초과 → 패널 효율 저하 → 가격 보정
  """

  # 1. 일사량 계수 (0 ~ 1.0)
  # 일사량 최대값 기준 3.0 MJ/m² 가정
  # 일사량 높을수록 공급 많아서 가격 낮춤
  MAX_SI = 3.0
  si_factor = min(solar_radiation / MAX_SI, 1.0)  # 0 ~ 1.0

  #2. 수급 비율 계수
  # 공급 > 수요 -> 1.0 이하 -> 가격 낮춤
  # 공급 < 수요 -> 1.0 초과 -> 가격 높임
  if generation_kwh > 0:
    supply_demand_ratio = demand_kwh / generation_kwh
  else:
    supply_demand_ratio = 2.0 # 공급 없으면 최대 가격

  # 3. 기온 보정 계수
  # 25°C 초과 시 1°C당 0.5% 효율 저하
  # 기온이 높으면 패널 효율이 떨어져서 발전량이 줄어들기 때문에 가격이 올라감
  if temperature > OPTIMAL_TEMP:
    temp_penalty = 1.0 + (temperature - OPTIMAL_TEMP) * 0.005
  else:
    temp_penalty = 1.0

  # 4. 최종 가격 계산
  # 기본가격 x (1 - 일사량계수 x 0.3) x 수급비율 x 기온보정
  price = BASE_PRICE * (1 - si_factor * 0.3) * supply_demand_ratio * temp_penalty
  price = round(price, 2)

  # 가격 범위 제한 (최소 50원, 최대 300원)
  return max(50.0, min(300.0, price))

def get_city_price(city: str, generation_kwh: float, demand_kwh: float) -> dict:
  """도시별 실시간 가격 산출"""
  weather_list = get_solar_radiation(city)

  if not weather_list:
    # 기상 데이터 없으면 기본 가격
    return {
      "city": city,
      "price": BASE_PRICE,
      "reason": "기상 데이터 없음 - 기본 가격 적응"
    }
  
  weather = weather_list[0]
  price = calculate_price(
    generation_kwh=generation_kwh,
    demand_kwh=demand_kwh,
    solar_radiation=weather['solar_radiation_si'],
    temperature=weather['temperature_ta']
  )

  return {
    "city": city,
    "price": price,
    "solar_radiation": weather['solar_radiation_si'],
    "temperature": weather['temperature_ta'],
    "generation_kwh": generation_kwh,
    "demand_kwh": demand_kwh,
    "observed_at": weather['observed_at']
  }

if __name__ == "__main__":
  from data_generator import generate_generation_data, generate_demand_data

  print("Dynamic Pricing 테스트...")

  gen_list = generate_generation_data(5)
  dem_list = generate_demand_data(5)

  cities = ["서울", "부산", "인천", "대구", "대전"]

  for i, city in enumerate(cities):
    result = get_city_price(
      city=city,
      generation_kwh=gen_list[i]['generation_kwh'],
      demand_kwh=dem_list[i]['demand_kwh'],
    )
    print(f"{result['city']} | 가격: {result['price']}원/kWh | "
          f"일사량: {result['solar_radiation']} MJ/m² | "
          f"기온: {result['temperature']}°C | "
          f"공급: {result['generation_kwh']} kWh | "
          f"수요: {result['demand_kwh']} kWh")

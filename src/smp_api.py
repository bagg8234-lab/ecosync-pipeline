import requests
import os
from datetime import datetime

API_KEY = os.getenv('KPX_API_KEY')

def get_smp(date: str = None) -> dict:
  """
  한국전력거래소 SMP(계통한계가격) 조회
  - data: YYYYMMDD 형식, 없으면 오늘 날짜
  - 반환 : 육지/제주 시간별 SMP 평균
  """
  if not date:
    date = datetime.now().strftime("%Y%m%d")

  url = "https://apis.data.go.kr/B552115/SmpWithForecastDemand/getSmpWithForecastDemand"
  params = {
    "serviceKey": API_KEY,
    "pageNo": "1",
    "numOfRows": "24",
    "dataType": "JSON",
    "date": date
  }

  try:
      response = requests.get(url, params=params, timeout=10)
      response.raise_for_status()
      data = response.json()

      items = data['response']['body']['items']['item']

      # 육지 SMP만 필터링 (areaName이 없거나 육지일때)
      land_items = [i for i in items if i.get('areaName', '육지') == '육지']

      if not land_items:
         land_items = items

      # 현재 시간 SMP 가져오기
      current_hour = str(datetime.now().hour)
      current_smp = None

      for item in land_items:
         if item.get('hour') == current_hour:
            current_smp = float(item.get('smp', 0))
            break
         
      # 현재 시간 데이터 없으면 평균값 사용
      if not current_smp:
         smp_values = [float(i.get('smp',0)) for i in land_items if i.get('smp')]
         current_smp = round(sum(smp_values) / len(smp_values), 2) if smp_values else 150.0

      return {
         "date": date,
         "hour": current_hour,
         "smp": current_smp,
         "unit": "원/kWh"
      }
  
  except Exception as e:
     print(f"SMP API 호출 실패: {e} -> 기본값 150.0 사용")
     return {
        "date": date,
        "hour": str(datetime.now().hour),
        "smp": 150.0,
        "unit": "원/kWh"
     }
  
if __name__ == "__main__":
    result = get_smp()
    print(f"SMP 조회 완료: {result['smp']} {result['unit']} ({result['date']} {result['hour']}시)")
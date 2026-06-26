import requests
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv('C:/TOY/ecosync-project/.env')
API_KEY=os.getenv('KPX_API_KEY')

# 우리 도시 -> API 지역명 매핑
CITY_TO_REGION = {
  "서울": "서울시",
  "부산": "부산시",
  "인천": "인천시",
  "대구": "대구시",
  "대전": "대전시",
}

def get_solar_generation() -> list[dict]:
  """
  한국전력거래소 지역별 시간별 태양광 발전량 조회
  - tradeYmd 없이 호출 → 최신 데이터 반환
  - 반환: 도시별 현재 시간 발전량
  """

  url = "https://apis.data.go.kr/B552115/PvAmountByLocHr/getPvAmountByLocHr"
  params = {
      "serviceKey": API_KEY,
      "pageNo": 1,
      "numOfRows": 500,
      "dataType": "JSON",
  }

  try:
      response = requests.get(url, params=params, timeout=10)
      response.raise_for_status()
      data = response.json()

      items = data['response']['body']['items']['item']
      current_hour = 12

      results = []
      for city, region in CITY_TO_REGION.items():
        city_data = [
            i for i in items
            if i.get('regionNm') == region
            and int(i.get('tradeNo', 0)) == current_hour
        ]

        if city_data:
          amgo = float(city_data[0].get('amgo', 0))
        else:
          amgo = 0.0

        # 날짜는 items 첫 번째 항목에서 가져오기
        trade_date = items[0].get('tradeYmd') if items else None

        results.append({
          "city": city,
          "region": region,
          "generation_kwh": amgo,
          "trade_date": trade_date,
          "hour": current_hour,
        })
        
      return results
  
  except Exception as e:
    print(f"발전량 API 호출 실패:{e}")
    return []
  
if __name__ == "__main__":
  data = get_solar_generation()
  if data:
    print(f"태양광 발전량 조회 완료\n")
    for d in data:
      print(f"{d['city']} | 발전량: {d['generation_kwh']} kWh | {d['trade_date']} {d['hour']}시")
  else:
      print("데이터 없음")

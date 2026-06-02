import math
import psycopg2
from datetime import datetime
from db_client import get_connection, create_tables

def haversine(lat1, lon1, lat2, lon2) -> float:
  """
  두 위도/경도 사이의 거리 계산(km)
  지구 곡률을 반영한 Haversine 공식
  """
  R = 6371 # 지구 반지름(km)

  # 위도/경도를 라디안으로 변환
  lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

  dlat = lat2 - lat1
  dlon = lon2 - lon1

  a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
  c = 2 * math.asin(math.sqrt(a))

  return R * c

def match_generation_to_demand_optimized(generation_list: list[dict], demand_list: list[dict]) -> list[dict]:
    trades = []
    available = {g['id']: g['generation_kwh'] for g in generation_list}

    # 1. [핵심 최적화] 각 수요자별로 모든 공급자와의 거리를 미리 계산해서 정렬해 둠 (단 1번만 수행)
    precomputed_distances = {}
    for demand in demand_list:
        distances = []
        for gen in generation_list:
            dist = haversine(
                demand['location_lat'], demand['location_lon'],
                gen['location_lat'], gen['location_lon']
            )
            distances.append((dist, gen))
        # 거리순으로 정렬해서 저장
        distances.sort(key=lambda x: x[0])
        precomputed_distances[demand['id']] = distances

    # 2. 매칭 루프 (더 이상 루프 내부에서 정렬이나 하버사인 계산을 하지 않음)
    for demand in demand_list:
        needed = demand['demand_kwh']
        
        # 미리 계산된 거리순 공급자 목록 가져오기
        sorted_gen_with_dist = precomputed_distances[demand['id']]

        for distance, gen in sorted_gen_with_dist:
            # 공급자의 에너지량이 0이하일 경우 
            if available[gen['id']] <= 0:
                continue

            matched_kwh = min(needed, available[gen['id']])
            available[gen['id']] -= matched_kwh
            needed -= matched_kwh

            trades.append({
                "generation_id": gen['id'],
                "demand_id": demand['id'],
                "generation_city": gen['city'],
                "demand_city": demand['city'],
                "matched_kwh": round(matched_kwh, 2),
                "distance_km": round(distance, 2), 
                "matched_at": datetime.now().isoformat()
            })

            if needed <= 0:
                break

    return trades

def save_trades(trades: list[dict]):
  """매칭 결과를 PostgreSQL trades 테이블에 저장"""
  conn = get_connection()
  cur = conn.cursor()

  cur.execute("""
              CREATE TABLE IF NOT EXISTS trades (
              id SERIAL PRIMARY KEY,
              generation_id VARCHAR(36),
              demand_id VARCHAR(36),
              generation_city VARCHAR(50),
              demand_city VARCHAR(50),
              matched_kwh FLOAT,
              distance_km FLOAT,
              matched_At TIMESTAMP
          )
      """)

  for trade in trades:
    cur.execute("""
                INSERT INTO trades
                (generation_id, demand_id, generation_city, demand_city, matched_kwh, distance_km, matched_at)
                VALUES
                (%(generation_id)s, %(demand_id)s, %(generation_city)s, %(demand_city)s,
                %(matched_kwh)s, %(distance_km)s, %(matched_at)s)
      """, trade)
    
    conn.commit()
    cur.close()
    conn.close()
    print(f"거래 {len(trades)}건 저장 완료")

if __name__ == "__main__":
  from data_generator import generate_generation_data, generate_demand_data
  from validator import validate_generation, validate_demand

  print("매칭 엔진 테스트")

  # 데이터 생성 및 검증
  gen_list = [d for d in generate_generation_data(10)
              if validate_generation(d)[0]]
  dem_list = [d for d in generate_demand_data(10)
              if validate_demand(d)[0]]
  
  # 매칭 실행
  trades = match_generation_to_demand_optimized(gen_list, dem_list)

  print((f"매칭 결과: {len(trades)}건"))
  for t in trades[:3]:
    print(f" {t['generation_city']} -> {t['demand_city']} | {t['matched_kwh']} kWh | {t['distance_km']} km")

  # DB 저장
  save_trades(trades)
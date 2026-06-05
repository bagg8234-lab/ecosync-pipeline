import math
import psycopg2
from datetime import datetime
from db_client import get_connection


def haversine(lat1, lon1, lat2, lon2) -> float:
    """
    두 위도/경도 사이의 거리 계산 (km)
    지구 곡률을 반영한 Haversine 공식
    """
    R = 6371  # 지구 반지름 (km)

    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))

    return R * c


def match_generation_to_demand(generation_list: list[dict], demand_list: list[dict]) -> tuple[list, list]:
    """
    발전량(공급)과 소비량(수요)을 거리 기준으로 매칭
    반환: (거래 목록, 매칭 실패 목록)
    """
    trades = []
    available = {g['id']: g['generation_kwh'] for g in generation_list}

    # 수요자별로 공급자 거리 미리 계산 (최적화)
    precomputed_distances = {}
    for demand in demand_list:
        distances = []
        for gen in generation_list:
            dist = haversine(
                demand['location_lat'], demand['location_lon'],
                gen['location_lat'], gen['location_lon']
            )
            distances.append((dist, gen))
        distances.sort(key=lambda x: x[0])
        precomputed_distances[demand['id']] = distances

    # 매칭 루프
    for demand in demand_list:
        needed = demand['demand_kwh']
        sorted_gen_with_dist = precomputed_distances[demand['id']]

        for distance, gen in sorted_gen_with_dist:
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

    # 매칭 실패 감지 — 수요가 남아있으면 에러 로그
    unmatched = []
    for demand in demand_list:
        matched_total = sum(
            t['matched_kwh'] for t in trades
            if t['demand_id'] == demand['id']
        )
        remaining = demand['demand_kwh'] - matched_total
        if remaining > 0:
            unmatched.append({
                "demand_id": demand['id'],
                "demand_city": demand['city'],
                "demand_kwh": demand['demand_kwh'],
                "unmatched_kwh": round(remaining, 2),
                "reason": "공급 가용량 부족"
            })

    return trades, unmatched


def save_trades(trades: list[dict], unmatched: list[dict]):
    """거래 결과 및 매칭 실패 로그를 PostgreSQL에 저장"""
    conn = get_connection()
    cur = conn.cursor()

    # trades 테이블 생성
    cur.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id SERIAL PRIMARY KEY,
            generation_id VARCHAR(36),
            demand_id VARCHAR(36),
            generation_city VARCHAR(50),
            demand_city VARCHAR(50),
            matched_kwh FLOAT,
            distance_km FLOAT,
            matched_at TIMESTAMP
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

    # matching_errors 테이블 생성
    cur.execute("""
        CREATE TABLE IF NOT EXISTS matching_errors (
            id SERIAL PRIMARY KEY,
            demand_id VARCHAR(36),
            demand_city VARCHAR(50),
            demand_kwh FLOAT,
            unmatched_kwh FLOAT,
            reason VARCHAR(200),
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)

    for item in unmatched:
        cur.execute("""
            INSERT INTO matching_errors
            (demand_id, demand_city, demand_kwh, unmatched_kwh, reason)
            VALUES (%(demand_id)s, %(demand_city)s, %(demand_kwh)s, %(unmatched_kwh)s, %(reason)s)
        """, item)

    conn.commit()
    cur.close()
    conn.close()

    print(f"거래 {len(trades)}건 저장 완료")
    if unmatched:
        print(f"매칭 실패 로그 {len(unmatched)}건 저장 완료")


if __name__ == "__main__":
    from data_generator import generate_generation_data, generate_demand_data
    from validator import validate_generation, validate_demand

    print("매칭 엔진 테스트 ...")

    gen_list = [d for d in generate_generation_data(10)
                if validate_generation(d)[0]]
    dem_list = [d for d in generate_demand_data(10)
                if validate_demand(d)[0]]

    print(f"공급 데이터: {len(gen_list)}개")
    print(f"수요 데이터: {len(dem_list)}개")

    trades, unmatched = match_generation_to_demand(gen_list, dem_list)

    print(f"매칭 결과: {len(trades)}건")
    for t in trades[:3]:
        print(f"  {t['generation_city']} → {t['demand_city']} | {t['matched_kwh']} kWh | {t['distance_km']} km")

    if unmatched:
        print(f"\n매칭 실패: {len(unmatched)}건")
        for u in unmatched:
            print(f"  {u['demand_city']} | 미충족: {u['unmatched_kwh']} kWh")

    save_trades(trades, unmatched)
import math
from collections import defaultdict
from datetime import datetime
import psycopg2
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


def _record_date(record: dict) -> str:
    """
    레코드의 event time(timestamp)에서 날짜(YYYY-MM-DD)만 추출.
    처리 시점(배치가 언제 실행됐는지)과 무관하게, 데이터가 실제로 발생한 날짜 기준.
    """
    return datetime.fromisoformat(record["timestamp"]).date().isoformat()


def match_generation_to_demand(generation_list: list[dict], demand_list: list[dict]) -> tuple[list, list]:
    """
    발전량(공급)과 소비량(수요)을 거리 기준으로 매칭.
    배치가 카운트 기준(10건)으로 트리거 -> record의 실제
    timestamp 날짜로 먼저 그룹을 나누고, 같은 날짜 그룹 안에서만 매칭.
    (매칭 시점을 뜻하는 matched_at과는 별개의 기준.)
    반환: (거래 목록, 매칭 실패 목록)
    """
    gen_by_date = defaultdict(list)
    for g in generation_list:
        gen_by_date[_record_date(g)].append(g)

    dem_by_date = defaultdict(list)
    for d in demand_list:
        dem_by_date[_record_date(d)].append(d)

    all_dates = set(gen_by_date) | set(dem_by_date)

    trades = []
    unmatched = []

    for trade_date in sorted(all_dates):
        day_generation = gen_by_date.get(trade_date, [])
        day_demand = dem_by_date.get(trade_date, [])

        available = {g['id']: g['generation_kwh'] for g in day_generation}

        # 수요자별로 공급자 거리 미리 계산 (최적화)
        precomputed_distances = {}
        for demand in day_demand:
            distances = []
            for gen in day_generation:
                dist = haversine(
                    demand['location_lat'], demand['location_lon'],
                    gen['location_lat'], gen['location_lon']
                )
                distances.append((dist, gen))
            distances.sort(key=lambda x: x[0])
            precomputed_distances[demand['id']] = distances

        # 매칭 루프 (같은 날짜 그룹 내에서만)
        for demand in day_demand:
            needed = demand['demand_kwh']
            sorted_gen_with_dist = precomputed_distances[demand['id']]

            for distance, gen in sorted_gen_with_dist:
                if available[gen['id']] <= 0:
                    continue

                matched_kwh = round(min(needed, available[gen['id']]), 2)
                if matched_kwh <= 0:
                    continue
                available[gen['id']] -= matched_kwh
                needed -= matched_kwh

                trades.append({
                    "generation_id": gen['id'],
                    "demand_id": demand['id'],
                    "generation_city": gen['city'],
                    "demand_city": demand['city'],
                    "matched_kwh": matched_kwh,
                    "distance_km": round(distance, 2),
                    "trade_date": trade_date,             # 데이터가 실제로 발생한 날짜
                    "matched_at": datetime.now().isoformat(),  # 매칭이 처리된 시점
                })

                if needed <= 0:
                    break

        # 매칭 실패 감지 — 이 날짜 그룹 안에서 수요가 남아있으면 에러 로그
        for demand in day_demand:
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
                    "trade_date": trade_date,
                    "reason": "공급 가용량 부족",
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
            trade_date DATE,
            matched_at TIMESTAMP
        )
    """)

    for trade in trades:
        cur.execute("""
            INSERT INTO trades
            (generation_id, demand_id, generation_city, demand_city, matched_kwh, distance_km, trade_date, matched_at)
            VALUES
            (%(generation_id)s, %(demand_id)s, %(generation_city)s, %(demand_city)s,
             %(matched_kwh)s, %(distance_km)s, %(trade_date)s, %(matched_at)s)
        """, trade)

    # matching_errors 테이블 생성
    cur.execute("""
        CREATE TABLE IF NOT EXISTS matching_errors (
            id SERIAL PRIMARY KEY,
            demand_id VARCHAR(36),
            demand_city VARCHAR(50),
            demand_kwh FLOAT,
            unmatched_kwh FLOAT,
            trade_date DATE,
            reason VARCHAR(200),
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)

    for item in unmatched:
        cur.execute("""
            INSERT INTO matching_errors
            (demand_id, demand_city, demand_kwh, unmatched_kwh, trade_date, reason)
            VALUES (%(demand_id)s, %(demand_city)s, %(demand_kwh)s, %(unmatched_kwh)s, %(trade_date)s, %(reason)s)
        """, item)

    conn.commit()
    cur.close()
    conn.close()

    print(f"거래 {len(trades)}건 저장 완료")
    if unmatched:
        print(f"매칭 실패 로그 {len(unmatched)}건 저장 완료")
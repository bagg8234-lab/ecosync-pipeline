"""
EcoSync 데이터 마트 배치 적재 스크립트

generation / demand / trades / matching_errors 4개 raw 테이블을
daily_city_trading_summary 마트 테이블로 집계 적재한다.

사용법 (레포 루트 기준, docker-compose 컨테이너 안에서 실행):
  docker exec -it ecosync-app python src/run_daily_mart_batch.py                    # 어제 날짜 적재
  docker exec -it ecosync-app python src/run_daily_mart_batch.py --date 2026-07-30  # 특정일 재적재
  docker exec -it ecosync-app python src/run_daily_mart_batch.py --backfill 30      # 최근 30일 재적재 (최초 1회용)
"""

import argparse
import os
from datetime import date, timedelta

from db_client import get_connection

SQL_PATH = os.path.join(
    os.path.dirname(__file__), "..", "sql", "daily_city_trading_summary_upsert.sql"
)


def load_upsert_sql() -> str:
    with open(SQL_PATH, "r", encoding="utf-8") as f:
        return f.read()


def run_batch_for_date(target_date: date, upsert_sql: str) -> None:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(upsert_sql, {"target_date": target_date})
        conn.commit()
        print(f"[OK] {target_date} 마트 적재 완료")
    except Exception as e:
        conn.rollback()
        print(f"[FAIL] {target_date} 마트 적재 실패: {e}")
        raise
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="EcoSync 일별 마트 배치 적재")
    parser.add_argument("--date", type=str, help="YYYY-MM-DD, 특정일만 재적재")
    parser.add_argument("--backfill", type=int, help="최근 N일 재적재 (최초 1회 과거 데이터 채울 때 사용)")
    args = parser.parse_args()

    upsert_sql = load_upsert_sql()

    if args.date:
        target_dates = [date.fromisoformat(args.date)]
    elif args.backfill:
        today = date.today()
        target_dates = [today - timedelta(days=i + 1) for i in range(args.backfill)]
    else:
        target_dates = [date.today() - timedelta(days=1)]

    for d in target_dates:
        run_batch_for_date(d, upsert_sql)


if __name__ == "__main__":
    main()
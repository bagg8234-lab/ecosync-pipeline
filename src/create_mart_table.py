"""
마트 테이블(daily_city_trading_summary) 생성 스크립트 — 최초 1회 실행

사용법:
  docker exec -it ecosync-app python src/create_mart_table.py
"""

import os

from db_client import get_connection

SQL_PATH = os.path.join(os.path.dirname(__file__), "..", "sql", "daily_city_trading_summary.sql")


def main():
    with open(SQL_PATH, "r", encoding="utf-8") as f:
        ddl = f.read()

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
        print("[OK] daily_city_trading_summary 테이블 생성 완료")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
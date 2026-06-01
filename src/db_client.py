import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    """PostgreSQL 연결"""
    return psycopg2.connect(
        host='db',
        port=5432,
        database=os.getenv('DB_NAME', 'ecosync_db'),
        user=os.getenv('DB_USER', 'user'),
        password=os.getenv('DB_PASSWORD', 'password')
    )

def create_table():
    """테이블 생성 - 초기 실행 시 한 번만 호출"""
    conn = get_connection()
    cur = conn.cursor()

    # 발전량 테이블
    cur.execute("""
        CREATE TABLE IF NOT EXISTS generation (
                id VARCHAR(36) PRIMARY KEY,
                timestamp TIMESTAMP,
                city VARCHAR(50),
                location_lat FLOAT,
                location_lon FLOAT,
                generation_kwh FLOAT,
                created_at TIMESTAMP DEFAULT NOW()
            )
    """)
    
    # 소비량 테이블
    cur.execute("""
            CREATE TABLE IF NOT EXISTS demand (
                id VARCHAR(36) PRIMARY KEY,
                timestamp TIMESTAMP,
                city VARCHAR(50),
                location_lat FLOAT,
                location_lon FLOAT,
                demand_kwh FLOAT,
                created_at TIMESTAMP DEFAULT NOW()
            )
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("테이블 생성 완료")

def upsert_generation(data:dict):
    """
    발전량 데이터 UPSERT
    - id가 존재하면 UPDATE, 없으면 INSERT
    - 재처리 시 데이터 중복 방지
    """
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO generation (id, timestamp, city, location_lat, location_lon, generation_kwh)
        VALUES (%(id)s, %(timestamp)s, %(city)s, %(location_lat)s, %(location_lon)s, %(generation_kwh)s)
        ON CONFLICT (id) DO UPDATE SET
            generation_kwh = EXCLUDED.generation_kwh,
            timestamp = EXCLUDED.timestamp
    """, data)

    conn.commit()
    cur.close()
    conn.close()
    print("발전량 데이터 UPSERT 완료")

def upsert_demand(data:dict):
    """소비량 데이터 UPSERT"""
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO demand (id, timestamp, city, location_lat, location_lon, demand_kwh)
        VALUES (%(id)s, %(timestamp)s, %(city)s, %(location_lat)s, %(location_lon)s, %(demand_kwh)s)
        ON CONFLICT (id) DO UPDATE SET
            demand_kwh = EXCLUDED.demand_kwh,
            timestamp = EXCLUDED.timestamp
    """, data)

    conn.commit()
    cur.close()
    conn.close()
    print("소비량 데이터 UPSERT 완료")

if __name__ == "__main__":
    from data_generator import generate_generation_data, generate_demand_data
    from validator import validate_generation, validate_demand

    create_table()

    print("PostgreSQL 데이터베이스 테스트")
    data_list = generate_generation_data(5)

    for data in data_list:
        is_valid, error = validate_generation(data)
        if is_valid:
            upsert_generation(data)
            print(f"적재 완료: {data['city']} | {data['generation_kwh']} kWh")
        else:
            print(f"검증 실패: {error}")
        
    print("\n같은 데이터 다시 적재 (UPSERT 테스트)")
    for data in data_list:
        upsert_generation(data)  # 동일 데이터 재적재 - UPSERT로 업데이트
        print(f"UPSERT 완료 (중복 없음) {data['city']} | {data['generation_kwh']} kWh")

    print("\n중복 없이 완료")
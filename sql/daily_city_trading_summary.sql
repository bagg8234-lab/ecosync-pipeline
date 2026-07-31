-- ============================================================
-- EcoSync 데이터 마트: daily_city_trading_summary
--
-- 목적:
--   generation(발전량)/demand(소비량) 수집기가 각각 독립된 프로세스로
--   실행되는 구조라, raw 테이블을 "최근 N건"처럼 서로 다른 기준으로
--   각각 조회해 비교하면 실제로는 서로 다른 시간대 데이터를 비교하게 될
--   위험이 있다. 일자(city, date) 단위로 미리 집계해두어 모든 화면이
--   동일한 시간 경계·동일한 계산 로직을 참조하도록 한다.
--
-- 생성 방법:
--   docker exec -it ecosync-app python src/create_mart_table.py
-- ============================================================

CREATE TABLE IF NOT EXISTS daily_city_trading_summary (
    summary_date              DATE          NOT NULL,
    city                      VARCHAR(50)   NOT NULL,

    total_generation_kwh      FLOAT   DEFAULT 0,
    total_demand_kwh          FLOAT   DEFAULT 0,

    matched_trade_count       INT     DEFAULT 0,
    matched_kwh                FLOAT  DEFAULT 0,
    avg_distance_km            FLOAT,
    max_distance_km            FLOAT,

    unmatched_error_count      INT    DEFAULT 0,
    unmatched_kwh              FLOAT  DEFAULT 0,

    -- 파생 지표: 모든 대시보드/리포트가 재계산하지 않고 이 값만 참조
    match_rate_kwh_pct         FLOAT,
    match_rate_count_pct       FLOAT,

    updated_at                 TIMESTAMP DEFAULT NOW(),

    PRIMARY KEY (summary_date, city)
);

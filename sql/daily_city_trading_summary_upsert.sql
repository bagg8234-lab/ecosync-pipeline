-- ============================================================
-- daily_city_trading_summary 배치 적재 (UPSERT)
-- :target_date 파라미터에 집계 대상일을 바인딩해서 실행
-- ============================================================

INSERT INTO daily_city_trading_summary (
    summary_date, city,
    total_generation_kwh, total_demand_kwh,
    matched_trade_count, matched_kwh, avg_distance_km, max_distance_km,
    unmatched_error_count, unmatched_kwh,
    match_rate_kwh_pct, match_rate_count_pct,
    updated_at
)
WITH gen AS (
    SELECT city, DATE(timestamp) AS d, SUM(generation_kwh) AS total_generation_kwh
    FROM generation
    WHERE DATE(timestamp) = %(target_date)s
    GROUP BY city, DATE(timestamp)
),
dem AS (
    SELECT city, DATE(timestamp) AS d, SUM(demand_kwh) AS total_demand_kwh
    FROM demand
    WHERE DATE(timestamp) = %(target_date)s
    GROUP BY city, DATE(timestamp)
),
trd AS (
    SELECT demand_city AS city, DATE(matched_at) AS d,
           COUNT(*) AS matched_trade_count,
           SUM(matched_kwh) AS matched_kwh,
           AVG(distance_km) AS avg_distance_km,
           MAX(distance_km) AS max_distance_km
    FROM trades
    WHERE DATE(matched_at) = %(target_date)s
    GROUP BY demand_city, DATE(matched_at)
),
err AS (
    SELECT demand_city AS city, DATE(created_at) AS d,
           COUNT(*) AS unmatched_error_count,
           SUM(unmatched_kwh) AS unmatched_kwh
    FROM matching_errors
    WHERE DATE(created_at) = %(target_date)s
    GROUP BY demand_city, DATE(created_at)
),
merged AS (
    SELECT
        COALESCE(gen.d, dem.d, trd.d, err.d)              AS summary_date,
        COALESCE(gen.city, dem.city, trd.city, err.city)  AS city,
        COALESCE(gen.total_generation_kwh, 0)             AS total_generation_kwh,
        COALESCE(dem.total_demand_kwh, 0)                 AS total_demand_kwh,
        COALESCE(trd.matched_trade_count, 0)              AS matched_trade_count,
        COALESCE(trd.matched_kwh, 0)                      AS matched_kwh,
        trd.avg_distance_km,
        trd.max_distance_km,
        COALESCE(err.unmatched_error_count, 0)            AS unmatched_error_count,
        COALESCE(err.unmatched_kwh, 0)                     AS unmatched_kwh
    FROM gen
    FULL OUTER JOIN dem ON gen.city = dem.city AND gen.d = dem.d
    FULL OUTER JOIN trd ON COALESCE(gen.city, dem.city) = trd.city AND COALESCE(gen.d, dem.d) = trd.d
    FULL OUTER JOIN err ON COALESCE(gen.city, dem.city, trd.city) = err.city AND COALESCE(gen.d, dem.d, trd.d) = err.d
)
SELECT
    summary_date, city,
    total_generation_kwh, total_demand_kwh,
    matched_trade_count, matched_kwh, avg_distance_km, max_distance_km,
    unmatched_error_count, unmatched_kwh,
    CASE WHEN (matched_kwh + unmatched_kwh) > 0
         THEN ROUND((matched_kwh / (matched_kwh + unmatched_kwh) * 100)::numeric, 2)
         ELSE NULL END AS match_rate_kwh_pct,
    CASE WHEN (matched_trade_count + unmatched_error_count) > 0
         THEN ROUND((matched_trade_count::numeric / (matched_trade_count + unmatched_error_count) * 100), 2)
         ELSE NULL END AS match_rate_count_pct,
    NOW() AS updated_at
FROM merged
ON CONFLICT (summary_date, city)
DO UPDATE SET
    total_generation_kwh   = EXCLUDED.total_generation_kwh,
    total_demand_kwh       = EXCLUDED.total_demand_kwh,
    matched_trade_count    = EXCLUDED.matched_trade_count,
    matched_kwh            = EXCLUDED.matched_kwh,
    avg_distance_km        = EXCLUDED.avg_distance_km,
    max_distance_km        = EXCLUDED.max_distance_km,
    unmatched_error_count  = EXCLUDED.unmatched_error_count,
    unmatched_kwh          = EXCLUDED.unmatched_kwh,
    match_rate_kwh_pct     = EXCLUDED.match_rate_kwh_pct,
    match_rate_count_pct   = EXCLUDED.match_rate_count_pct,
    updated_at             = NOW();

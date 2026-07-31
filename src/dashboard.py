import streamlit as st
import pandas as pd
from datetime import date, timedelta
from dynamic_pricing import get_city_price
from data_generator import generate_generation_data, generate_demand_data
from db_client import get_connection as get_db_connection

# 페이지 설정
st.set_page_config(
    page_title="EcoSync 대시보드",
    page_icon="⚡",
    layout="wide"
)

st.title("⚡ EcoSync - 실시간 에너지 거래 대시보드")


# ------------------------------------------------------------
# 실시간 스트림용 raw 조회 (최근 N건)
# "지금 이 순간 무슨 일이 일어나고 있는지" 보는 용도라 마트로 바꾸지 않음
# ------------------------------------------------------------

@st.cache_data(ttl=10)  # 10초마다 갱신 (time.sleep과 달리 코드가 멈추지 않음)
def load_trades():
    conn = get_db_connection()
    df = pd.read_sql("SELECT * FROM trades ORDER BY matched_at DESC LIMIT 50", conn)
    conn.close()
    return df


@st.cache_data(ttl=10)
def load_generation():
    conn = get_db_connection()
    df = pd.read_sql("SELECT * FROM generation ORDER BY timestamp DESC LIMIT 50", conn)
    conn.close()
    return df


@st.cache_data(ttl=10)
def load_demand():
    conn = get_db_connection()
    df = pd.read_sql("SELECT * FROM demand ORDER BY timestamp DESC LIMIT 50", conn)
    conn.close()
    return df


# ------------------------------------------------------------
# 일자별 집계 조회 — daily_city_trading_summary 마트에서 조회
# raw 테이블을 그때그때 다른 LIMIT/시간창으로 각각 긁어와 비교하던 방식(수급비율,
# 매칭 성공/실패 집계)은 서로 다른 시간대 데이터를 비교하게 되는 문제가 있어
# 날짜 기준으로 미리 정렬된 마트 테이블을 단일 소스로 사용하도록 교체함.
# ------------------------------------------------------------

@st.cache_data(ttl=60)
def load_mart_summary(start_date: date, end_date: date) -> pd.DataFrame:
    conn = get_db_connection()
    df = pd.read_sql(
        """
        SELECT *
        FROM daily_city_trading_summary
        WHERE summary_date BETWEEN %(start_date)s AND %(end_date)s
        ORDER BY summary_date, city
        """,
        conn,
        params={"start_date": start_date, "end_date": end_date},
    )
    conn.close()
    return df


# 1. 도시별 실시간 가격
st.header("📊 도시별 실시간 가격")

gen_list = generate_generation_data(5)
dem_list = generate_demand_data(5)
cities = ["서울", "부산", "인천", "대구", "대전"]

price_data = []
for i, city in enumerate(cities):
    result = get_city_price(
        city=city,
        generation_kwh=gen_list[i]['generation_kwh'],
        demand_kwh=dem_list[i]['demand_kwh'],
    )
    price_data.append(result)

price_df = pd.DataFrame(price_data)
cols = ['city', 'price']
for col in ['solar_radiation', 'temperature', 'generation_kwh', 'demand_kwh']:
    if col in price_df.columns:
        cols.append(col)
price_df = price_df[cols]
price_df.columns = ['도시', '가격(원/kWh)'] + ['일사량(MJ/m²)', '기온(°C)', '공급(kWh)', '수요(kWh)'][: len(cols) - 2]
st.dataframe(price_df, use_container_width=True, hide_index=True)


# 2. 발전량 현황 
st.header("☀️ 발전량 현황 (최근 50건)")
gen_df = load_generation()
if not gen_df.empty:
    st.bar_chart(gen_df.groupby('city')['generation_kwh'].mean())


# ------------------------------------------------------------
# 날짜 range 선택 (마트 기반 섹션 공통으로 사용)
# ------------------------------------------------------------
st.divider()
col_a, col_b = st.columns(2)
with col_a:
    range_start = st.date_input("조회 시작일", value=date.today() - timedelta(days=6))
with col_b:
    range_end = st.date_input("조회 종료일", value=date.today())

mart_df = load_mart_summary(range_start, range_end)

# 3. 지역별 수급 현황 — 마트 기반 (raw 최근 50건 비교 → 동일 일자 기준 비교로 교체)
st.header("🗺️ 지역별 수급 현황 (일자별 마트 기준)")
if not mart_df.empty:
    supply_demand = (
        mart_df.groupby('city')[['total_generation_kwh', 'total_demand_kwh']]
        .sum()
        .reset_index()
    )
    supply_demand['수급비율'] = (
        supply_demand['total_demand_kwh'] / supply_demand['total_generation_kwh']
    ).round(2)
    supply_demand.columns = ['도시', '총 발전량(kWh)', '총 소비량(kWh)', '수급비율']

    st.dataframe(supply_demand, use_container_width=True, hide_index=True)
    st.bar_chart(supply_demand.set_index('도시')[['총 발전량(kWh)', '총 소비량(kWh)']])
else:
    st.info("선택한 기간에 마트 데이터가 없어요.")


# 4. 최근 거래 체결 현황
st.header("🔄 최근 거래 체결 현황 (최근 50건)")
trades_df = load_trades()
if not trades_df.empty:
    st.dataframe(trades_df, use_container_width=True, hide_index=True)

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("총 거래 건수", len(trades_df))
    with col2:
        st.metric("평균 거래량", f"{trades_df['matched_kwh'].mean():.1f} kWh")
    with col3:
        st.metric("평균 거래 거리", f"{trades_df['distance_km'].mean():.1f} km")
else:
    st.info("거래 데이터가 없어요. 매칭 엔진을 먼저 실행해주세요.")


# 5. Validator 처리 현황 — 마트 기반 (전체 누적 카운트 → 선택 기간 기준으로 교체)
st.header("✅ Validator 처리 현황 (선택 기간 기준)")
if not mart_df.empty:
    trades_count = int(mart_df['matched_trade_count'].sum())
    errors_count = int(mart_df['unmatched_error_count'].sum())
    total = trades_count + errors_count

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("총 처리 건수", total)
    with col2:
        st.metric("매칭 성공", trades_count)
    with col3:
        st.metric("매칭 실패", errors_count)

    chart_data = pd.DataFrame({
        "상태": ["매칭 성공", "매칭 실패"],
        "건수": [trades_count, errors_count]
    })
    st.bar_chart(chart_data.set_index("상태"))

    # 매칭률도 마트에서 이미 계산된 값을 그대로 사용
    if total > 0:
        overall_match_rate = round(trades_count / total * 100, 2)
        st.caption(f"기간 내 매칭 성공률: {overall_match_rate}%")
else:
    st.info("선택한 기간에 마트 데이터가 없어요.")
import streamlit as st
import pandas as pd
import psycopg2
from dynamic_pricing import get_city_price
from data_generator import generate_generation_data, generate_demand_data

# 페이지 설정
st.set_page_config(
  page_title="EcoSync 대시보드",
  page_icon="⚡",
  layout="wide"
)

st.title("⚡ EcoSync - 실시간 에너지 거래 대시보드")

def get_db_connection():
  return psycopg2.connect(
    host='db', port=5432,
    database='ecosync_db',
    user='user', password='password'
  )

# 이 함수의 결과물을 10초 동안 기억해 둬(캐싱)
# time.sleep(10): 이건 10초동안 코드가 멈춰버리는 것
@st.cache_data(ttl=10) # 10초마다 갱신
def load_trades():
  """거래 대시보드 조회"""
  conn = get_db_connection()
  df = pd.read_sql("SELECT * FROM trades ORDER BY matched_at DESC LIMIT 50", conn)
  conn.close()
  return df

@st.cache_data(ttl=10)
def load_generation():
    """발전량 데이터 조회"""
    conn = get_db_connection()
    df = pd.read_sql("SELECT * FROM generation ORDER BY timestamp DESC LIMIT 50", conn)
    conn.close()
    return df

# 1. 실시간 가격 섹션
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

price_df = pd.DataFrame(price_data)[['city', 'price', 'solar_radiation', 'temperature', 'generation_kwh', 'demand_kwh']]
price_df.columns = ['도시', '가격(원/kWh)', '일사량(MJ/m²)', '기온(°C)', '공급(kWh)', '수요(kWh)']
st.dataframe(price_df, use_container_width=True)

# 2. 거래 현황 섹션
st.header("🔄 최근 거래 체결 현황")
trades_df = load_trades()
if not trades_df.empty:
  st.dataframe(trades_df, use_container_width=True) # 화면 가로 폭에 맞춰서 꽉 차게 채움

  # 화면을 3개의 열로 나누기
  col1, col2, col3 = st.columns(3)
  with col1:
    st.metric("총 거래 건수", len(trades_df))
  with col2:
    st.metric("평균 거래량", f"{trades_df['matched_kwh'].mean():.1f} kWh")
  with col3:
    st.metric("평균 거래 거리", f"{trades_df['distance_km'].mean():.1f} km")
else:
  st.info("거래 데이터가 없어요. 매칭 엔진을 먼저 실행해주세요.")

# 3. 발전량 현황
st.header("☀️ 발전량 현황")
gen_df = load_generation()
if not gen_df.empty:
    st.bar_chart(gen_df.groupby('city')['generation_kwh'].mean())

@st.cache_data(ttl=10)
def load_matching_errors():
  conn = get_db_connection()
  df = pd.read_sql("SELECT * FROM matching_errors ORDER BY created_at DESC LIMIT 50", conn)
  conn.close()
  return df

@st.cache_data(ttl=10)
def load_demand():
  conn=get_db_connection()
  df = pd.read_sql("SELECT * FROM demand ORDER BY timestamp DESC LIMIT 50", conn)
  conn.close()
  return df

# 4. Validator 통과/실패 비율
st.header("✅ Validator 처리 현황")
errors_df = load_matching_errors()
trades_df = load_trades()

total = len(trades_df) + len(errors_df)
if total > 0:
  co1, col2, col3 = st.columns(3)
  with col1:
    st.metric("총 거리 건수", total)
  with col2:
    st.metric("매칭 성공", len(trades_df))
  with col3:
    st.metric("매칭 실패", len(errors_df))

  # 비율 차트
  chart_data = pd.DataFrame({
    "상태": ["매칭 성공", "매칭 실패"],
    "건수": [len(trades_df), len(errors_df)]
  })
  st.bar_chart(chart_data.set_index("상태"))

# 5. 지역별 수급 현황 산점도
# 5. 지역별 수급 현황
st.header("🗺️ 지역별 수급 현황")

gen_df = load_generation()
dem_df = load_demand()

if not gen_df.empty and not dem_df.empty:
    # reset_index → 인덱스를 컬럼으로
    gen_by_city = gen_df.groupby('city')['generation_kwh'].mean().reset_index()
    dem_by_city = dem_df.groupby('city')['demand_kwh'].mean().reset_index()

    supply_demand = pd.merge(gen_by_city, dem_by_city, on='city')
    supply_demand['수급비율'] = (supply_demand['demand_kwh'] / supply_demand['generation_kwh']).round(2)

    supply_demand.columns = ['도시', '평균 발전량(kWh)', '평균 소비량(kWh)', '수급비율']

    st.dataframe(supply_demand, use_container_width=True)
    # set_index   → 컬럼을 인덱스로
    st.bar_chart(supply_demand.set_index('도시')[['평균 발전량(kWh)', '평균 소비량(kWh)']])
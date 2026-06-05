import requests
from datetime import datetime, timedelta

# 도시별 기상청 지점번호
CITY_STN = {
    "서울": 108,
    "부산": 159,
    "인천": 112,
    "대구": 143,
    "대전": 133,
}

# 지점번호 → 도시이름 역산용
STN_TO_CITY = {v: k for k, v in CITY_STN.items()}

API_KEY = "MWb0_NxZRfOm9PzcWRXz9g"


def get_solar_radiation(city: str = None, tm: str = None) -> list[dict]:
    """
    기상청 API 허브 - 지상 기상관측 자료 수집 및 파싱
    """
    # 1. 인자가 들어왔는지 안 들어왔는지부터 명확하게 분리
    if city:  
        # 도시를 입력했는데, 우리 시스템(CITY_STN)에 등록된 정당한 도시인 경우
        if city in CITY_STN:
            stn = str(CITY_STN[city])
        else:
            # 엉뚱한 도시를 넣으면 빈 값을 주어 방어
            print(f"⚠️ [경고] '{city}'는 지원하지 않는 지점입니다. 수집을 중단합니다.")
            return [] 
    else:
        # 아예 인자를 비워두고 호출한 경우에만 전체 도시를 한방에 긁어옴
        stn = ":".join(str(v) for v in CITY_STN.values())

    # 기상청 반영 속도를 고려하여 15분 전 시간으로 타임스탬프 생성
    safe_time = datetime.now() - timedelta(minutes=15)
    tm_str = safe_time.strftime("%Y%m%d%H00")

    # help : 1은 설명글로 주석 줄 추가, 0은 주석 모두 생략과 데이터와 데이터 이름표
    params = {
        "tm": tm_str, 
        "stn": stn,
        "help": 0,
        "authKey": API_KEY,
    }

    try:
        response = requests.get(
            "https://apihub.kma.go.kr/api/typ01/url/kma_sfctm2.php",
            params=params,
            timeout=10,
    )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"❌ API 호출 실패 (네트워크/HTTP 에러): {e}")
        return []

    headers = []
    results = []

    # 줄바꿈 기준으로 한줄씩 보기 
    for line in response.text.strip().split("\n"):
        line = line.strip()
        # 비어있거나 "조회 종료"같은 안내 문구이거나 기상청 특유의 시작과 끝을 알리는 데이터 구분선이면 패스
        if not line or "종료" in line or line in ("#START7777", "#7777END"):
            continue
        # 기상청은 날씨 데이터를 주기 직전 줄에 변수 이름표를 줌
        # #을 지우고 공백 기준으로 쪼갬
        # header list : ['YYMMDDHHMI', 'STN', 'TA', 'SI']
        if line.startswith("#") and "YYMMDDHHMI" in line:
            headers = line.replace("#", "").strip().split()
            continue
        # 이름표 줄이 아닌 그냥 기상청 안내 설명문이 적힌 다른 주석 줄들은 전부 패스
        if line.startswith("#"):
            continue
        
        if headers:
            # values 리스트: ['202606040100', '108', '24.5', '2.84']
            values = line.split()
            # 데이터 뒤쪽 공백이나 다른 값이 붙어있을까봐 정확히 이름효 개수만큼 자르기
            # 그리고 zip을 활용하여 딕셔너리 형식으로 결합
            if len(values) >= len(headers):
                d = dict(zip(headers, values[:len(headers)]))
                si = d.get("SI", "0.0")
                ta = d.get("TA", "0.0")
                stn_id = int(d.get("STN", 0))
                results.append({
                    "city": STN_TO_CITY.get(stn_id, "Unknown"),
                    "stn_id": stn_id,
                    "observed_at": d.get("YYMMDDHHMI"),
                    # 기상청에서 결측치를 -9로 표시
                    "solar_radiation_si": float(si) if float(si) > 0 else 0.0,
                    "temperature_ta": float(ta) if float(ta) > 0 else 0.0,
                })

    return results


if __name__ == "__main__":
    weather_data = get_solar_radiation()
    print(f"기상 데이터 조회 완료: {len(weather_data)}개 도시\n")
    for d in weather_data:
        print(f" 도시: {d['city']} (지점: {d['stn_id']})")
        print(f" 관측시각: {d['observed_at']}")
        print(f" 일사량(SI): {d['solar_radiation_si']} MJ/m²")
        print(f" 현재기온(TA): {d['temperature_ta']} °C")
        print("-" * 40)
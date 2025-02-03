import time
import requests
from datetime import datetime, timedelta

# API 설정 (이전 제공 값 그대로 사용)
POLYGON_API_KEY = "lsstdMdFXY50qjPNMQrXFp4vAGj0bNd5"
AIRTABLE_API_KEY = "patBy8FRWWiG6P99a.a0670e9dd25c84d028c9f708af81d5f1fb164c3adeb1cee067d100075db8b748"
AIRTABLE_BASE_ID = "appAh82iPV3cH6Xx5"
TABLE_NAME = "미국주식 데이터"

# 1. Polygon Snapshot API 호출 - 모든 주식 티커의 최신 시세 데이터 가져오기
snapshot_url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?apiKey={POLYGON_API_KEY}"
response = requests.get(snapshot_url)
if response.status_code != 200:
    raise Exception(f"Polygon Snapshot API 실패: HTTP {response.status_code}")
data = response.json()

tickers_data = data.get("tickers", [])
print(f"총 {len(tickers_data)}개 종목의 스냅샷 데이터를 받았습니다.")

# 1차 필터링 결과를 저장할 리스트
filtered_tickers = []

# Snapshot 데이터 1차 필터링: 가격 >= $10, 거래량 >= 1,000,000
for ticker_info in tickers_data:
    ticker = ticker_info.get("ticker")
    day_data = ticker_info.get("day", {})
    last_trade = ticker_info.get("lastTrade", {})

    # 현재 가격과 거래량 추출 (시장 시간에 따라 lastTrade 혹은 day의 c 활용)
    price = last_trade.get("p") or day_data.get("c")  # 현재가 (p: last trade price)
    volume = day_data.get("v")                        # 당일 거래량 (v: volume)

    # 데이터 검증: price나 volume이 None인 경우 스킵
    if price is None or volume is None:
        print(f"[스킵] {ticker}: Snapshot에 가격 또는 거래량 데이터가 없음.")
        continue

    # 1차 기준 충족 여부 확인
    if price < 10:
        print(f"[필터제외] {ticker}: 현재가 ${price:.2f} (< $10 기준 미달)")
        continue
    if volume < 1_000_000:
        print(f"[필터제외] {ticker}: 거래량 {volume:,}주 (< 1,000,000주 기준 미달)")
        continue

    # 1차 조건 통과: 시가총액 조건은 이후에 체크
    filtered_tickers.append({
        "ticker": ticker,
        "price": price,
        "volume": volume
    })

print(f"1차 필터링 통과 종목: {len(filtered_tickers)}개")

# Polygon Free Tier Rate Limit 준수를 위해 일시 대기
time.sleep(1)

# 1차 통과 리스트에 대해 Ticker Details API를 호출하여 시가총액 필터 적용
final_candidates = []
for item in filtered_tickers:
    ticker = item["ticker"]
    details_url = f"https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey={POLYGON_API_KEY}"
    resp = requests.get(details_url)
    if resp.status_code != 200:
        print(f"[경고] {ticker}: Ticker Details API 호출 실패 (HTTP {resp.status_code}), 해당 종목 제외")
        continue
    details = resp.json().get("results", {})
    market_cap = details.get("market_cap")
    name = details.get("name")
    primary_exchange = details.get("primary_exchange")

    if market_cap is None:
        print(f"[경고] {ticker}: 시가총액 정보 없음 (제외)")
        continue

    if market_cap < 500_000_000:
        print(f"[필터제외] {ticker}: 시가총액 ${market_cap:,} (< $500M 기준 미달)")
        continue

    item.update({
        "name": name if name else "",
        "market_cap": market_cap,
        "exchange": primary_exchange if primary_exchange else ""
    })
    final_candidates.append(item)
    time.sleep(0.2)

print(f"시가총액 조건 포함 1차 최종 통과 종목: {len(final_candidates)}개")

# 2. 52주 최고가 계산 및 2차 필터링
selected_stocks = []
today = datetime.today()
start_date = (today - timedelta(days=365)).strftime("%Y-%m-%d")
end_date = today.strftime("%Y-%m-%d")

for stock in final_candidates:
    ticker = stock["ticker"]
    price = stock["price"]

    # Aggregates API 호출 (지난 365일 일봉 데이터 조회)
    agg_url = (f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/"
               f"{start_date}/{end_date}?adjusted=true&apiKey={POLYGON_API_KEY}")
    agg_resp = requests.get(agg_url)
    if agg_resp.status_code != 200:
        print(f"[경고] {ticker}: Aggregates API 호출 실패 (HTTP {agg_resp.status_code}), 종목 제외")
        continue
    agg_data = agg_resp.json()
    results = agg_data.get("results")
    if not results:
        print(f"[경고] {ticker}: 지난 1년치 데이터 없음 또는 결과 비어있음, 종목 제외")
        continue

    # 52주 최고가 계산 (일봉 high 값 중 최대)
    year_high = max([bar.get("h", 0) for bar in results if bar.get("h") is not None], default=0)
    if year_high == 0:
        print(f"[경고] {ticker}: 1년 최고가 계산 실패 (high값 0), 종목 제외")
        continue

    # 현재가가 52주 최고가의 95% 이상인지 확인
    ratio = price / year_high
    if ratio >= 0.95:
        stock.update({
            "year_high": year_high,
            "close_to_high_pct": ratio * 100  # 퍼센트로 표현
        })
        selected_stocks.append(stock)
        print(f"[통과] {ticker}: 현재가 ${price:.2f}, 52주 최고가 ${year_high:.2f}, 비율 {ratio*100:.1f}%")
    else:
        print(f"[필터제외] {ticker}: 현재가 {ratio*100:.1f}% (52주 최고가 대비) < 95%")
    time.sleep(0.2)

print(f"2차 필터링(52주 고가 대비) 통과 종목 수: {len(selected_stocks)}개")

# 3. Airtable에 결과 추가
airtable_url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{TABLE_NAME}"
headers = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json"
}

   for stock in stock_data:
        try:
            day_data = stock.get('day', {})
            price = float(day_data.get('c', 0))
            record = {
                '티커': stock.get('ticker', ''),
                '종목명': stock.get('name', ''),
                '현재가': price,
                '52주 신고가': float(stock.get('52_week_high', 0)),
                '신고가대비': f"{stock.get('price_to_high_ratio', 0) * 100:.1f}%",
                '시가총액': f"${float(stock.get('market_cap', 0)):,.2f}",
                '업데이트 시간': current_date,
                '분류': category,
                '거래소': convert_exchange_code(stock.get('primary_exchange', 'N/A'))
            }
            airtable.insert(record)
            print(f"Airtable에 추가: {record['티커']}")
            time.sleep(0.2)  # Airtable API 호출 속도 조절
        except Exception as e:
            print(f"Airtable 데이터 추가 중 오류 ({stock.get('ticker', 'Unknown')}): {str(e)}")

print(f"Airtable 업데이트 완료: 총 {len(selected_stocks)}개 종목 신규 등록")

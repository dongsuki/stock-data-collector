import time
import requests
from datetime import datetime, timedelta

# ✅ API 설정
POLYGON_API_KEY = "lsstdMdFXY50qjPNMQrXFp4vAGj0bNd5"
AIRTABLE_API_KEY = "patBy8FRWWiG6P99a.a0670e9dd25c84d028c9f708af81d5f1fb164c3adeb1cee067d100075db8b748"
AIRTABLE_BASE_ID = "appAh82iPV3cH6Xx5"
TABLE_NAME = "미국주식 데이터"

# ✅ 1. 모든 주식 데이터 가져오기
def get_all_stocks():
    url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?apiKey={POLYGON_API_KEY}"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Polygon Snapshot API 실패: HTTP {response.status_code}")
    data = response.json()
    return data.get("tickers", [])

# ✅ 2. 종가, 거래량 필터링
def filter_stocks(tickers_data):
    filtered_tickers = []
    for ticker_info in tickers_data:
        ticker = ticker_info.get("ticker")
        day_data = ticker_info.get("day", {})
        last_trade = ticker_info.get("lastTrade", {})

        # 현재가와 거래량 추출
        price = last_trade.get("p") or day_data.get("c")
        volume = day_data.get("v")

        if price and volume:
            if price >= 10 and volume >= 1_000_000:
                filtered_tickers.append({"ticker": ticker, "price": price, "volume": volume})

    return filtered_tickers

# ✅ 3. 시가총액 필터링
def filter_by_market_cap(filtered_tickers):
    final_candidates = []
    for item in filtered_tickers:
        ticker = item["ticker"]
        details_url = f"https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey={POLYGON_API_KEY}"
        resp = requests.get(details_url)

        if resp.status_code == 200:
            details = resp.json().get("results", {})
            market_cap = details.get("market_cap")
            name = details.get("name")
            exchange = details.get("primary_exchange")

            if market_cap and market_cap >= 500_000_000:
                item.update({"name": name, "market_cap": market_cap, "exchange": exchange})
                final_candidates.append(item)

        time.sleep(0.2)  # API 호출 간격 조절

    return final_candidates

# ✅ 4. 52주 최고가 계산
def get_52_week_high(ticker):
    end_date = datetime.today()
    start_date = end_date - timedelta(days=365)
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}?adjusted=true&apiKey={POLYGON_API_KEY}"
    
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json().get("results", [])
        if data:
            return max(day.get("h", 0) for day in data)

    return 0  # 52주 최고가 데이터 없음

# ✅ 5. 52주 신고가 대비 95% 이상 종목 필터링
def filter_by_52_week_high(final_candidates):
    selected_stocks = []
    for stock in final_candidates:
        ticker = stock["ticker"]
        year_high = get_52_week_high(ticker)
        if year_high > 0:
            ratio = stock["price"] / year_high
            if ratio >= 0.95:
                stock.update({"year_high": year_high, "close_to_high_pct": round(ratio * 100, 2)})
                selected_stocks.append(stock)

        time.sleep(0.2)  # API 호출 간격 조절
    
    return selected_stocks

# ✅ 6. Airtable에 결과 추가 (한글 필드명 적용)
def update_airtable(selected_stocks):
    airtable_url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{TABLE_NAME}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
        "Content-Type": "application/json"
    }

    current_date = datetime.now().strftime("%Y-%m-%d")

    for stock in selected_stocks:
        fields = {
            "티커": stock["ticker"],
            "종목명": stock.get("name", ""),
            "현재가": round(stock["price"], 2),
            "52주 신고가": round(stock["year_high"], 2),
            "신고가대비": f"{stock['close_to_high_pct']}%",
            "거래량": stock["volume"],
            "시가총액": f"${int(stock['market_cap']):,}",
            "업데이트 시간": current_date,
            "거래소": stock.get("exchange", "")
        }
        record_data = {"fields": fields}
        at_resp = requests.post(airtable_url, json=record_data, headers=headers)

        if at_resp.status_code == 200:
            print(f"[Airtable] {stock['ticker']} 추가 완료")
        else:
            print(f"[Airtable 오류] {stock['ticker']} (HTTP {at_resp.status_code}): {at_resp.text}")

# ✅ 7. 메인 실행 함수
def main():
    print("📌 데이터 수집 시작...")

    # 1. 모든 주식 데이터 가져오기
    tickers_data = get_all_stocks()
    print(f"총 {len(tickers_data)}개 종목의 스냅샷 데이터를 받았습니다.")

    # 2. 1차 필터링 (종가, 거래량)
    filtered_tickers = filter_stocks(tickers_data)
    print(f"1차 필터링 통과 종목: {len(filtered_tickers)}개")

    # 3. 1차 통과 종목 중 시가총액 500M 이상 필터링
    final_candidates = filter_by_market_cap(filtered_tickers)
    print(f"시가총액 조건 포함 최종 1차 통과 종목: {len(final_candidates)}개")

    # 4. 52주 최고가 대비 95% 이상인 종목 필터링
    selected_stocks = filter_by_52_week_high(final_candidates)
    print(f"2차 필터링(52주 고가 대비) 통과 종목 수: {len(selected_stocks)}개")

    # 5. Airtable에 결과 추가
    if selected_stocks:
        update_airtable(selected_stocks)
        print(f"Airtable 업데이트 완료: 총 {len(selected_stocks)}개 종목 신규 등록")
    else:
        print("🔴 선택된 종목이 없습니다.")

    print("✅ 모든 데이터 처리 완료!")

# ✅ 실행
if __name__ == "__main__":
    main()

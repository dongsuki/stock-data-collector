import os
import requests
from datetime import datetime, timedelta
from airtable import Airtable
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# API 설정
POLYGON_API_KEY = "lsstdMdFXY50qjPNMQrXFp4vAGj0bNd5"
AIRTABLE_API_KEY = "patBy8FRWWiG6P99a.a0670e9dd25c84d028c9f708af81d5f1fb164c3adeb1cee067d100075db8b748"
AIRTABLE_BASE_ID = "appAh82iPV3cH6Xx5"
TABLE_NAME = "미국주식 데이터"

def convert_exchange_code(mic):
    """거래소 코드를 읽기 쉬운 형태로 변환"""
    exchange_map = {
        'XNAS': 'NASDAQ',
        'XNYS': 'NYSE',
        'XASE': 'AMEX'
    }
    return exchange_map.get(mic, mic)

def get_52_week_high(ticker):
    """Polygon.io Aggregates API를 사용하여 52주 신고가 계산"""
    end_date = datetime.now()
    start_date = end_date - timedelta(weeks=52)
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}"
    params = {'adjusted': 'true', 'apiKey': POLYGON_API_KEY}

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        if 'results' in data:
            high_prices = [day['h'] for day in data['results']]  # 일별 최고가 리스트
            return max(high_prices)  # 최고값 반환
        return 0.0
    except requests.exceptions.RequestException:
        return 0.0

def calculate_52_week_high_parallel(tickers):
    """병렬 처리로 52주 신고가 계산"""
    results = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_ticker = {executor.submit(get_52_week_high, ticker): ticker for ticker in tickers}
        for future in as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            try:
                results[ticker] = future.result()
                print(f"{ticker}: 계산된 52주 신고가 = {results[ticker]}")  # 디버깅 로그
            except Exception as e:
                print(f"{ticker}: 계산 실패 - {e}")
                results[ticker] = 0.0
    return results

def filter_52_week_high_stocks(stocks):
    """52주 신고가 상위 주식 필터링"""
    filtered = []
    missing_high_stocks = [stock for stock in stocks if stock.get('52w_high', 0.0) == 0.0]
    tickers = [stock['ticker'] for stock in missing_high_stocks]

    # 병렬 처리로 누락된 52주 신고가 계산
    high_values = calculate_52_week_high_parallel(tickers)

    for stock in stocks:
        ticker = stock['ticker']
        day_data = stock.get('day', {})
        current_price = float(day_data.get('c', 0))  # 현재 가격
        high_52_week = stock.get('52w_high', 0.0) or high_values.get(ticker, 0.0)

        # 디버깅 로그
        print(f"Ticker: {ticker}, Current Price: {current_price}, 52-Week High: {high_52_week}")

        # 조건: 현재가 >= 52주 최고가
        if high_52_week > 0 and current_price >= high_52_week:
            stock['52_week_high'] = high_52_week
            filtered.append(stock)

    return filtered

def update_airtable(stock_data, category):
    """Airtable에 데이터 추가"""
    airtable = Airtable(AIRTABLE_BASE_ID, TABLE_NAME, AIRTABLE_API_KEY)
    current_date = datetime.now().strftime("%Y-%m-%d")

    for stock in stock_data:
        try:
            day_data = stock.get('day', {})
            record = {
                '티커': stock.get('ticker', ''),
                '종목명': stock.get('name', ''),
                '현재가': float(day_data.get('c', 0)),
                '52주 신고가': float(stock.get('52_week_high', 0)),
                '업데이트 시간': current_date,
                '분류': category
            }
            if stock.get('primary_exchange'):
                record['거래소 정보'] = convert_exchange_code(stock['primary_exchange'])

            airtable.insert(record)
            print(f"새 데이터 추가 완료: {record['티커']} ({category})")
            time.sleep(0.2)
        except Exception as e:
            print(f"레코드 처리 중 에러 발생 ({stock.get('ticker', 'Unknown')}): {str(e)}")

def main():
    print("데이터 수집 시작...")

    # Polygon.io Snapshot API 호출
    url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
    params = {'apiKey': POLYGON_API_KEY, 'include_otc': False}

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        all_stocks = response.json().get('tickers', [])

        print(f"\n총 {len(all_stocks)}개 종목 데이터 수집됨")

        # 52주 신고가 상위 필터링
        filtered_stocks = filter_52_week_high_stocks(all_stocks)
        print(f"\n조건을 만족하는 종목 수: {len(filtered_stocks)}개")

        # Airtable 업로드
        if filtered_stocks:
            update_airtable(filtered_stocks, "52주 신고가 상위")

        print("\n모든 데이터 처리 완료!")

    except Exception as e:
        print(f"데이터 수집 중 에러 발생: {e}")

if __name__ == "__main__":
    main()

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
    """
    Polygon.io Aggregates API를 사용하여 52주 신고가 계산
    (에러 핸들링 및 데이터 구조 검증 강화)
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(weeks=52)
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}"
    params = {'adjusted': 'true', 'apiKey': POLYGON_API_KEY}

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"[{ticker}] API 요청 실패: {e}")
        return 0.0

    try:
        data = response.json()
    except ValueError as e:
        print(f"[{ticker}] JSON 파싱 실패: {e}")
        return 0.0

    results = data.get('results')
    if not results or not isinstance(results, list):
        print(f"[{ticker}] 유효한 결과 데이터가 없습니다.")
        return 0.0

    try:
        high_prices = [day.get('h', 0.0) for day in results if day.get('h') is not None]
        if not high_prices:
            print(f"[{ticker}] 데이터에 'h' 값이 없습니다.")
            return 0.0
        return max(high_prices)
    except Exception as e:
        print(f"[{ticker}] 52주 신고가 계산 중 오류 발생: {e}")
        return 0.0

def calculate_52_week_high_parallel(tickers):
    """
    병렬 처리로 52주 신고가 계산
    (각 티커별로 API 호출하여 결과를 딕셔너리 형태로 반환)
    """
    results = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_ticker = {executor.submit(get_52_week_high, ticker): ticker for ticker in tickers}
        for future in as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            try:
                results[ticker] = future.result()
                print(f"{ticker}: 계산된 52주 신고가 = {results[ticker]}")
            except Exception as e:
                print(f"{ticker}: 계산 실패 - {e}")
                results[ticker] = 0.0
    return results

def filter_52_week_high_stocks(stocks):
    """
    52주 신고가 상위 주식 필터링
    - 기존 데이터에 '52w_high' 값이 없으면 병렬 계산한 값을 사용
    - 현재가(day['c'])가 52주 신고가 이상인 종목만 필터링
    """
    filtered = []
    # '52w_high' 정보가 없는 종목들의 티커 추출
    missing_high_stocks = [stock for stock in stocks if float(stock.get('52w_high', 0.0)) == 0.0]
    tickers = [stock.get('ticker') for stock in missing_high_stocks if stock.get('ticker')]
    
    # 병렬 처리로 누락된 52주 신고가 계산
    high_values = {}
    if tickers:
        high_values = calculate_52_week_high_parallel(tickers)

    for stock in stocks:
        ticker = stock.get('ticker')
        if not ticker:
            continue

        # 현재가(day['c']) 가져오기 (오류 발생 시 0으로 처리)
        try:
            day_data = stock.get('day', {})
            current_price = float(day_data.get('c', 0))
        except (ValueError, TypeError):
            current_price = 0.0

        # 기존에 저장된 '52w_high'가 있다면 사용, 없으면 병렬 처리 결과 사용
        try:
            stored_high = float(stock.get('52w_high', 0.0))
        except (ValueError, TypeError):
            stored_high = 0.0
        high_52_week = stored_high if stored_high > 0 else high_values.get(ticker, 0.0)

        print(f"Ticker: {ticker}, Current Price: {current_price}, 52-Week High: {high_52_week}")

        # 조건: 현재가가 52주 신고가 이상인 경우
        if high_52_week > 0 and current_price >= high_52_week:
            stock['52_week_high'] = high_52_week  # Airtable 업데이트용으로 저장
            filtered.append(stock)

    return filtered

def update_airtable(stock_data, category):
    """
    Airtable에 데이터 추가 (필드명: '티커', '종목명', '현재가', '52주 신고가', '업데이트 시간', '분류', '거래소 정보')
    """
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
            print(f"레코드 처리 중 에러 발생 ({stock.get('ticker', 'Unknown')}): {e}")

def main():
    print("데이터 수집 시작...")

    # Polygon.io Snapshot API 호출: 미국 상장 전체 주식 데이터 조회
    url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
    params = {'apiKey': POLYGON_API_KEY, 'include_otc': False}

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        all_stocks = data.get('tickers', [])
        print(f"\n총 {len(all_stocks)}개 종목 데이터 수집됨")
    except Exception as e:
        print(f"데이터 수집 중 에러 발생: {e}")
        return

    # 52주 신고가 상위 필터링 (기존 코드 흐름 유지)
    filtered_stocks = filter_52_week_high_stocks(all_stocks)
    print(f"\n조건을 만족하는 종목 수: {len(filtered_stocks)}개")

    # Airtable 업로드 (기존 필드명 그대로 사용)
    if filtered_stocks:
        update_airtable(filtered_stocks, "52주 신고가 상위")

    print("\n모든 데이터 처리 완료!")

if __name__ == "__main__":
    main()

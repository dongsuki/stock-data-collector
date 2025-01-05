from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import requests

POLYGON_API_KEY = "lsstdMdFXY50qjPNMQrXFp4vAGj0bNd5"

def get_52_week_high(ticker):
    """Polygon.io Aggregates API를 사용하여 52주 신고가 계산"""
    end_date = datetime.now()
    start_date = end_date - timedelta(weeks=52)  # 52주 전 날짜 계산

    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}"
    params = {'adjusted': 'true', 'apiKey': POLYGON_API_KEY}

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        if 'results' in data:
            high_prices = [day['h'] for day in data['results']]  # 각 날짜의 최고가 리스트
            return max(high_prices)  # 최고값 반환
        else:
            print(f"{ticker}: 데이터 없음")
            return 0.0

    except requests.exceptions.RequestException as e:
        print(f"{ticker} 데이터 가져오기 실패: {e}")
        return 0.0

def calculate_52_week_high_parallel(tickers):
    """병렬 처리로 52주 신고가 계산"""
    results = {}
    with ThreadPoolExecutor(max_workers=10) as executor:  # 병렬 처리 스레드 수
        future_to_ticker = {executor.submit(get_52_week_high, ticker): ticker for ticker in tickers}
        for future in as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            try:
                results[ticker] = future.result()
            except Exception as e:
                print(f"{ticker}: 계산 실패 - {e}")
                results[ticker] = 0.0
    return results

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

        # 누락된 52주 신고가 종목 필터링
        missing_high_stocks = [stock['ticker'] for stock in all_stocks if stock.get('52w_high', 0.0) == 0.0]
        print(f"누락된 52주 신고가 종목 수: {len(missing_high_stocks)}개")

        # 병렬 처리로 52주 신고가 계산
        if missing_high_stocks:
            print("\n52주 신고가 계산 시작...")
            high_values = calculate_52_week_high_parallel(missing_high_stocks)

            # 계산된 값 업데이트
            for stock in all_stocks:
                ticker = stock['ticker']
                if ticker in high_values:
                    stock['52w_high'] = high_values[ticker]

        # 52주 신고가 조건 필터링
        filtered_stocks = [stock for stock in all_stocks if stock.get('day', {}).get('c', 0) >= stock.get('52w_high', 0)]
        print(f"\n조건을 만족하는 종목 수: {len(filtered_stocks)}개")

    except Exception as e:
        print(f"데이터 수집 중 에러 발생: {e}")

if __name__ == "__main__":
    main()

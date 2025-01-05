import requests

POLYGON_API_KEY = "lsstdMdFXY50qjPNMQrXFp4vAGj0bNd5"

def check_52_week_high_availability(stocks):
    """52주 신고가 값이 누락된 종목 필터링"""
    missing_high_stocks = [stock for stock in stocks if stock.get('52w_high', 0.0) == 0.0]
    print(f"총 종목 수: {len(stocks)}개")
    print(f"누락된 52주 신고가 종목 수: {len(missing_high_stocks)}개")
    
    # 누락된 종목 일부 출력
    for stock in missing_high_stocks[:10]:  # 최대 10개만 출력
        print(f"누락된 종목: {stock['ticker']}, 현재 가격: {stock.get('day', {}).get('c', 'N/A')}")
    return missing_high_stocks

def main():
    print("데이터 수집 시작...")

    # Polygon.io Snapshot API 호출
    url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
    params = {'apiKey': POLYGON_API_KEY, 'include_otc': False}

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # HTTP 에러 발생 시 예외
        all_stocks = response.json().get('tickers', [])

        print(f"\n총 {len(all_stocks)}개 종목 데이터 수집됨")
        
        # 누락 여부 확인
        missing_high_stocks = check_52_week_high_availability(all_stocks)

    except Exception as e:
        print(f"데이터 수집 중 에러 발생: {e}")

if __name__ == "__main__":
    main()

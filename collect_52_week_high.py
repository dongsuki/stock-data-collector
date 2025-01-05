import os
import requests
from datetime import datetime
from airtable import Airtable
import time

# 환경 변수 설정
POLYGON_API_KEY = "your_polygon_api_key"
AIRTABLE_API_KEY = "your_airtable_api_key"
AIRTABLE_BASE_ID = "your_airtable_base_id"
TABLE_NAME = "미국주식 데이터"

def convert_exchange_code(mic):
    """거래소 코드를 읽기 쉬운 형태로 변환"""
    exchange_map = {
        'XNAS': 'NASDAQ',
        'XNYS': 'NYSE',
        'XASE': 'AMEX'
    }
    return exchange_map.get(mic, mic)

def get_all_stocks():
    """모든 주식 데이터 가져오기"""
    url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
    params = {
        'apiKey': POLYGON_API_KEY,
        'include_otc': False
    }
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            print(f"전체 데이터 샘플:", data['tickers'][0] if data.get('tickers') else 'No data')
            return data.get('tickers', [])
        else:
            print(f"API 요청 실패: {response.status_code}")
            return []
    except Exception as e:
        print(f"데이터 수집 중 에러 발생: {str(e)}")
        return []

def filter_52_week_high_stocks(stocks):
    """52주 최고가 상위 주식 필터링"""
    filtered = []
    total = len(stocks)
    print(f"총 {total}개 종목에서 52주 최고가 상위 주식 필터링 시작...")

    for i, stock in enumerate(stocks, 1):
        # 현재 데이터
        day_data = stock.get('day', {})
        current_price = float(day_data.get('c', 0))  # 현재 가격
        high_52_week = float(stock.get('52w_high', 0))  # 52주 최고가

        # 디버깅 로그 추가
        print(f"Ticker: {stock['ticker']}, Current Price: {current_price}, 52-Week High: {high_52_week}")

        # 조건: 52주 최고가 대비 현재가 상위
        if high_52_week > 0 and current_price >= 0.9 * high_52_week:  # 조건 수정
            stock['52_week_high'] = high_52_week
            filtered.append(stock)
    
    # 52주 최고가 기준 정렬 후 상위 20개 선택
    return sorted(filtered, key=lambda x: x.get('52_week_high', 0), reverse=True)[:20]

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
                '52주 최고가': float(stock.get('52_week_high', 0)),
                '거래량': int(day_data.get('v', 0)),
                '업데이트 시간': current_date,
                '분류': category
            }
            if stock.get('primary_exchange'):
                record['거래소 정보'] = convert_exchange_code(stock['primary_exchange'])
            
            # Airtable 업로드
            airtable.insert(record)
            print(f"새 데이터 추가 완료: {record['티커']} ({category})")
            time.sleep(0.2)  # API 호출 제한 방지
        except Exception as e:
            print(f"레코드 처리 중 에러 발생 ({stock.get('ticker', 'Unknown')}): {str(e)}")

def main():
    print("데이터 수집 시작...")

    # Polygon.io 데이터 수집
    all_stocks = get_all_stocks()
    if not all_stocks:
        print("데이터 수집 실패")
        return

    print(f"\n총 {len(all_stocks)}개 종목 데이터 수집됨")
    
    # 52주 최고가 상위 필터링
    filtered_stocks = filter_52_week_high_stocks(all_stocks)
    print(f"\n조건을 만족하는 종목 수: {len(filtered_stocks)}개")

    # Airtable 업로드
    if filtered_stocks:
        update_airtable(filtered_stocks, "52주 최고가 상위")
    
    print("\n모든 데이터 처리 완료!")

if __name__ == "__main__":
    main()

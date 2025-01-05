import os
import requests
from datetime import datetime
from airtable import Airtable
import time

# API 키와 Airtable 테이블 정보
POLYGON_API_KEY = "lsstdMdFXY50qjPNMQrXFp4vAGj0bNd5"  # Polygon API Key
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

        if i % 10 == 0:
            print(f"진행 중... {i}/{total}")
        
        # 조건: 52주 최고가 대비 현재가 상위
        if current_price >= 0.9 * high_52_week:
            stock['52_week_high'] = high_52_week
            filtered.append(stock)
    
    # 52주 최고가를 기준으로 정렬
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
            
            airtable.insert(record)
            print(f"새 데이터 추가 완료: {record['티커']} ({category})")
            time.sleep(0.2)
        except Exception as e:
            print(f"레코드 처리 중 에러 발생 ({stock.get('ticker', 'Unknown')}): {str(e)}")

def main():
    print("데이터 수집 시작...")

    all_stocks = get_all_stocks()
    if not all_stocks:
        print("데이터 수집 실패")
        return

    print(f"\n총 {len(all_stocks)}개 종목 데이터 수집됨")
    filtered_stocks = filter_52_week_high_stocks(all_stocks)
    
    print(f"\n조건을 만족하는 종목 수: {len(filtered_stocks)}개")
    if filtered_stocks:
        update_airtable(filtered_stocks, "52주 최고가 상위")

    print("\n모든 데이터 처리 완료!")

if __name__ == "__main__":
    main()

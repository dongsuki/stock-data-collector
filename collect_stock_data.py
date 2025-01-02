import os
import requests
from datetime import datetime
from airtable import Airtable
import time

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

def get_stock_details(ticker):
    """종목 상세정보 조회"""
    url = f"https://api.polygon.io/v3/reference/tickers/{ticker}"
    params = {'apiKey': POLYGON_API_KEY}
    
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response.json().get('results', {})
        return None
    except:
        return None

def get_gainers_data():
    """전일대비등락률상위 데이터"""
    url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/gainers"
    params = {'apiKey': POLYGON_API_KEY}
    return fetch_and_filter(url, params, "전일대비등락률상위", 
                          min_price=5, min_volume=1000000, min_change=5, min_market_cap=100000000)

def get_volume_leaders():
    """거래대금상위 데이터"""
    url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
    params = {
        'apiKey': POLYGON_API_KEY,
        'sort': 'volume',
        'order': 'desc',
        'limit': 20
    }
    return fetch_and_filter(url, params, "거래대금상위", 
                          min_price=5, min_market_cap=100000000)

def get_market_cap_leaders():
    """시가총액상위 데이터"""
    url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
    params = {
        'apiKey': POLYGON_API_KEY,
        'sort': 'market_cap',
        'order': 'desc',
        'limit': 20
    }
    return fetch_and_filter(url, params, "시가총액상위", 
                          min_price=5, min_volume=1000000)

def get_52_week_high():
    """52주 신고가 데이터"""
    url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/52-week-high"
    params = {
        'apiKey': POLYGON_API_KEY,
        'limit': 20
    }
    return fetch_and_filter(url, params, "52주신고가", 
                          min_price=5, min_volume=1000000, min_market_cap=100000000)

def fetch_and_filter(url, params, category, min_price=0, min_volume=0, min_change=0, min_market_cap=0):
    """데이터 가져오기 및 필터링"""
    try:
        response = requests.get(url, params=params)
        if response.status_code != 200:
            print(f"{category} 데이터 요청 실패: {response.status_code}")
            return []
            
        stocks = response.json().get('tickers', [])
        print(f"\n{category}: 초기 수집 {len(stocks)}개")
        
        filtered = []
        for stock in stocks:
            day_data = stock.get('day', {})
            price = float(day_data.get('c', 0))
            volume = int(day_data.get('v', 0))
            change = float(stock.get('todaysChangePerc', 0))
            
            # 기본 조건 체크
            if price < min_price or volume < min_volume or change < min_change:
                continue
                
            # 상세 정보 가져오기
            details = get_stock_details(stock['ticker'])
            if details:
                market_cap = float(details.get('market_cap', 0))
                if market_cap >= min_market_cap:
                    stock['name'] = details.get('name', '')
                    stock['market_cap'] = market_cap
                    stock['primary_exchange'] = details.get('primary_exchange', '')
                    filtered.append(stock)
                    print(f"추가됨: {stock['ticker']} (${price:.2f}, {volume:,}주)")
            
            time.sleep(0.1)
        
        print(f"{category}: 최종 선택 {len(filtered)}개")
        return filtered
        
    except Exception as e:
        print(f"{category} 데이터 수집 중 에러 발생: {str(e)}")
        return []

def update_airtable(stock_data, category):
    """Airtable에 데이터 업데이트"""
    airtable = Airtable(AIRTABLE_BASE_ID, TABLE_NAME, AIRTABLE_API_KEY)
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    for stock in stock_data:
        try:
            day_data = stock.get('day', {})
            
            record = {
                '티커': stock.get('ticker', ''),
                '종목명': stock.get('name', ''),
                '현재가': float(day_data.get('c', 0)),
                '등락률': float(stock.get('todaysChangePerc', 0)),
                '거래량': int(day_data.get('v', 0)),
                '시가총액': float(stock.get('market_cap', 0)),
                '업데이트 시간': current_date,
                '분류': category
            }
            
            if stock.get('primary_exchange'):
                record['거래소 정보'] = convert_exchange_code(stock['primary_exchange'])
            
            if not record['티커']:
                continue
                
            existing_records = airtable.search('티커', record['티커'])
            
            if existing_records:
                airtable.update(existing_records[0]['id'], record)
                print(f"업데이트: {record['티커']} ({category})")
            else:
                airtable.insert(record)
                print(f"신규추가: {record['티커']} ({category})")
            
            time.sleep(0.2)
            
        except Exception as e:
            print(f"에러 발생 ({stock.get('ticker', 'Unknown')}): {str(e)}")

def main():
    print("\n=== 미국 주식 데이터 수집 시작 ===")
    
    # 1. 전일대비등락률상위
    print("\n[전일대비등락률상위]")
    print("기준: 현재가 $5↑, 거래량 100만주↑, 등락률 5%↑, 시가총액 $1억↑")
    gainers = get_gainers_data()
    if gainers:
        update_airtable(gainers, "전일대비등락률상위")
        
    # 2. 거래대금상위
    print("\n[거래대금상위]")
    print("기준: 현재가 $5↑, 시가총액 $1억↑")
    volume_leaders = get_volume_leaders()
    if volume_leaders:
        update_airtable(volume_leaders, "거래대금상위")
        
    # 3. 시가총액상위
    print("\n[시가총액상위]")
    print("기준: 현재가 $5↑, 거래량 100만주↑")
    market_cap_leaders = get_market_cap_leaders()
    if market_cap_leaders:
        update_airtable(market_cap_leaders, "시가총액상위")
        
    # 4. 52주 신고가
    print("\n[52주 신고가]")
    print("기준: 현재가 $5↑, 거래량 100만주↑, 시가총액 $1억↑")
    high_52_week = get_52_week_high()
    if high_52_week:
        update_airtable(high_52_week, "52주신고가")
    
    print("\n=== 모든 데이터 처리 완료 ===")

if __name__ == "__main__":
    main()

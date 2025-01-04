import os
import requests
from datetime import datetime
from airtable import Airtable
import time

FMP_API_KEY = "EApxNJTRwcXOrhy2IUqSeKV0gyH8gans"
AIRTABLE_API_KEY = "patBy8FRWWiG6P99a.a0670e9dd25c84d028c9f708af81d5f1fb164c3adeb1cee067d100075db8b748"
AIRTABLE_BASE_ID = "appAh82iPV3cH6Xx5"
TABLE_NAME = "미국주식 데이터"

def get_gainers_data():
    """전일대비등락률상위 데이터"""
    url = f"https://financialmodelingprep.com/api/v3/stock_market/gainers"
    params = {'apikey': FMP_API_KEY}  # apiKey -> apikey
    
    try:
        response = requests.get(url, params=params)
        print(f"등락률상위 데이터 수집: {response.status_code}")
        
        if response.status_code == 200:
            stocks = response.json()
            print(f"수집된 전체 종목 수: {len(stocks)}")
            
            filtered = [stock for stock in stocks if (
                float(stock.get('price', 0)) >= 5 and
                float(stock.get('volume', 0)) >= 1000000 and
                float(stock.get('changesPercentage', 0)) >= 5
            )]
            print(f"필터링 후 종목 수: {len(filtered)}")
            return filtered
    except Exception as e:
        print(f"전일대비등락률상위 데이터 수집 중 에러: {str(e)}")
    return []

def get_volume_leaders():
    """거래대금상위 데이터"""
    url = f"https://financialmodelingprep.com/api/v3/stock_market/actives"
    params = {'apikey': FMP_API_KEY}  # apiKey -> apikey
    
    try:
        response = requests.get(url, params=params)
        print(f"거래대금상위 데이터 수집: {response.status_code}")
        
        if response.status_code == 200:
            stocks = response.json()
            print(f"수집된 전체 종목 수: {len(stocks)}")
            
            filtered = [stock for stock in stocks if float(stock.get('price', 0)) >= 5]
            for stock in filtered:
                stock['trading_value'] = float(stock.get('volume', 0)) * float(stock.get('price', 0))
            filtered = sorted(filtered, key=lambda x: x['trading_value'], reverse=True)[:20]
            print(f"필터링 후 종목 수: {len(filtered)}")
            return filtered
    except Exception as e:
        print(f"거래대금상위 데이터 수집 중 에러: {str(e)}")
    return []

def get_market_cap_leaders():
    """시가총액상위 데이터"""
    url = f"https://financialmodelingprep.com/api/v3/stock-screener"
    params = {
        'apikey': FMP_API_KEY,  # apiKey -> apikey
        'marketCapMoreThan': 100000000,
        'volumeMoreThan': 1000000,
        'priceMoreThan': 5,
        'limit': 20
    }
    
    try:
        response = requests.get(url, params=params)
        print(f"시가총액상위 데이터 수집: {response.status_code}")
        
        if response.status_code == 200:
            stocks = response.json()
            print(f"수집된 종목 수: {len(stocks)}")
            return sorted(stocks, key=lambda x: float(x.get('marketCap', 0)), reverse=True)[:20]
    except Exception as e:
        print(f"시가총액상위 데이터 수집 중 에러: {str(e)}")
    return []

def get_52_week_high():
    """52주 신고가 데이터"""
    url = f"https://financialmodelingprep.com/api/v3/stock_market/52_week_high"
    params = {'apikey': FMP_API_KEY}  # apiKey -> apikey
    
    try:
        response = requests.get(url, params=params)
        print(f"52주 신고가 데이터 수집: {response.status_code}")
        
        if response.status_code == 200:
            stocks = response.json()
            print(f"수집된 전체 종목 수: {len(stocks)}")
            
            filtered = [stock for stock in stocks if (
                float(stock.get('price', 0)) >= 5 and
                float(stock.get('volume', 0)) >= 1000000
            )]
            print(f"필터링 후 종목 수: {len(filtered)}")
            return filtered[:20]
    except Exception as e:
        print(f"52주 신고가 데이터 수집 중 에러: {str(e)}")
    return []

def update_airtable(stock_data, category):
    """Airtable에 데이터 추가"""
    airtable = Airtable(AIRTABLE_BASE_ID, TABLE_NAME, AIRTABLE_API_KEY)
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    print(f"\n{category} 데이터 Airtable 업데이트 시작")
    print(f"업데이트할 종목 수: {len(stock_data)}")
    
    for stock in stock_data:
        try:
            record = {
                '티커': stock.get('symbol', ''),
                '종목명': stock.get('name', stock.get('companyName', '')),
                '현재가': float(stock.get('price', 0)),
                '등락률': float(stock.get('changesPercentage', 0)),
                '거래량': int(stock.get('volume', 0)),
                '시가총액': float(stock.get('marketCap', 0)),
                '업데이트 시간': current_date,
                '분류': category
            }
            
            if stock.get('exchange'):
                record['거래소 정보'] = stock['exchange']
            
            print(f"추가 중: {record['티커']} ({category})")
            airtable.insert(record)
            time.sleep(0.2)
            
        except Exception as e:
            print(f"레코드 처리 중 에러 발생 ({stock.get('symbol', 'Unknown')}): {str(e)}")

def main():
    print("\n=== 미국 주식 데이터 수집 시작 ===")
    
    # 1. 전일대비등락률상위
    print("\n[전일대비등락률상위]")
    print("기준: 현재가 $5↑, 거래량 100만주↑, 등락률 5%↑")
    gainers = get_gainers_data()
    if gainers:
        update_airtable(gainers, "전일대비등락률상위")
        
    # 2. 거래대금상위
    print("\n[거래대금상위]")
    print("기준: 현재가 $5↑ (상위 20개)")
    volume_leaders = get_volume_leaders()
    if volume_leaders:
        update_airtable(volume_leaders, "거래대금상위")
        
    # 3. 시가총액상위
    print("\n[시가총액상위]")
    print("기준: 현재가 $5↑, 거래량 100만주↑ (상위 20개)")
    market_cap_leaders = get_market_cap_leaders()
    if market_cap_leaders:
        update_airtable(market_cap_leaders, "시가총액상위")
        
    # 4. 52주 신고가
    print("\n[52주 신고가]")
    print("기준: 현재가 $5↑, 거래량 100만주↑ (상위 20개)")
    high_52_week = get_52_week_high()
    if high_52_week:
        update_airtable(high_52_week, "52주신고가")
    
    print("\n=== 모든 데이터 처리 완료 ===")

if __name__ == "__main__":
    main()

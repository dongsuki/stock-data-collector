import os
import requests
from datetime import datetime
from airtable import Airtable
import time

FMP_API_KEY = "EApxNJTRwcXOrhy2IUqSeKV0gyH8gans"
AIRTABLE_API_KEY = "patBy8FRWWiG6P99a.a0670e9dd25c84d028c9f708af81d5f1fb164c3adeb1cee067d100075db8b748"
AIRTABLE_BASE_ID = "appAh82iPV3cH6Xx5"
TABLE_NAME = "미국주식 데이터"

def get_nasdaq_quotes():
    """NASDAQ 전체 시세"""
    return requests.get(f"https://financialmodelingprep.com/api/v3/quotes/nasdaq", 
                       params={'apikey': FMP_API_KEY}).json()

def get_nyse_quotes():
    """NYSE 전체 시세"""
    return requests.get(f"https://financialmodelingprep.com/api/v3/quotes/nyse", 
                       params={'apikey': FMP_API_KEY}).json()

def get_gainers_data():
    """전일대비등락률상위 데이터"""
    # NASDAQ과 NYSE 데이터 합치기
    all_quotes = get_nasdaq_quotes() + get_nyse_quotes()
    print(f"수집된 전체 종목 수: {len(all_quotes)}")
    
    # 조건 필터링
    filtered = [stock for stock in all_quotes if (
        float(stock.get('price', 0)) >= 5 and
        float(stock.get('volume', 0)) >= 1000000 and
        float(stock.get('changesPercentage', 0)) >= 5
    )]
    print(f"필터링 후 종목 수: {len(filtered)}")
    
    # 등락률 기준 정렬
    return sorted(filtered, key=lambda x: float(x['changesPercentage']), reverse=True)

def get_volume_leaders():
    """거래대금상위 데이터"""
    all_quotes = get_nasdaq_quotes() + get_nyse_quotes()
    
    # 거래대금 계산 및 $5 이상 필터링
    valid_stocks = []
    for stock in all_quotes:
        if float(stock.get('price', 0)) >= 5:
            stock['trading_value'] = float(stock['price']) * float(stock['volume'])
            valid_stocks.append(stock)
    
    # 거래대금 기준 정렬 후 상위 20개
    return sorted(valid_stocks, key=lambda x: x['trading_value'], reverse=True)[:20]

def get_market_cap_leaders():
    """시가총액상위 데이터"""
    # Market Screener API 사용
    url = "https://financialmodelingprep.com/api/v3/stock-screener"
    params = {
        'apikey': FMP_API_KEY,
        'marketCapMoreThan': 1000000000,  # 10억 달러 이상
        'limit': 100  # 충분히 큰 수로 설정
    }
    
    stocks = requests.get(url, params=params).json()
    
    # 시가총액 기준 정렬 후 상위 20개
    return sorted(stocks, key=lambda x: float(x.get('marketCap', 0)), reverse=True)[:20]

def get_52_week_high():
    """52주 신고가 데이터"""
    all_quotes = get_nasdaq_quotes() + get_nyse_quotes()
    
    # 조건 필터링
    filtered = []
    for stock in all_quotes:
        try:
            price = float(stock.get('price', 0))
            year_high = float(stock.get('yearHigh', 0))
            volume = float(stock.get('volume', 0))
            
            if (price >= 5 and 
                volume >= 1000000 and 
                year_high > 0 and 
                price >= year_high * 0.95):
                stock['high_ratio'] = price / year_high
                filtered.append(stock)
        except:
            continue
    
    # 52주 고가 대비 현재가 비율로 정렬
    return sorted(filtered, key=lambda x: x['high_ratio'], reverse=True)[:20]

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
                '종목명': stock.get('name', ''),
                '현재가': float(stock.get('price', 0)),
                '등락률': float(stock.get('changesPercentage', 0)),
                '거래량': int(stock.get('volume', 0)),
                '시가총액': float(stock.get('marketCap', 0)),
                '거래소 정보': stock.get('exchange', ''),
                '업데이트 시간': current_date,
                '분류': category
            }
            print(f"추가 중: {record['티커']} ({category})")
            airtable.insert(record)
            time.sleep(0.2)  # Airtable rate limit 고려
            
        except Exception as e:
            print(f"에러 발생 ({stock.get('symbol', 'Unknown')}): {str(e)}")

def main():
    print("\n=== 미국 주식 데이터 수집 시작 ===")
    
    print("\n[전일대비등락률상위]")
    gainers = get_gainers_data()
    if gainers:
        update_airtable(gainers, "전일대비등락률상위")
    
    print("\n[거래대금상위]")
    volume_leaders = get_volume_leaders()
    if volume_leaders:
        update_airtable(volume_leaders, "거래대금상위")
    
    print("\n[시가총액상위]")
    market_cap_leaders = get_market_cap_leaders()
    if market_cap_leaders:
        update_airtable(market_cap_leaders, "시가총액상위")
    
    print("\n[52주 신고가]")
    high_52_week = get_52_week_high()
    if high_52_week:
        update_airtable(high_52_week, "52주신고가")
    
    print("\n=== 모든 데이터 처리 완료 ===")

if __name__ == "__main__":
    main()

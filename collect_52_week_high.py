import os
import requests
from datetime import datetime
from airtable import Airtable
import time

FMP_API_KEY = "EApxNJTRwcXOrhy2IUqSeKV0gyH8gans"
AIRTABLE_API_KEY = "patBy8FRWWiG6P99a.a0670e9dd25c84d028c9f708af81d5f1fb164c3adeb1cee067d100075db8b748"
AIRTABLE_BASE_ID = "appAh82iPV3cH6Xx5"
TABLE_NAME = "미국주식 데이터"

def get_tradable_stocks():
    """거래 가능한 모든 주식 목록 조회"""
    url = f"https://financialmodelingprep.com/api/v3/available-traded/list?apikey={FMP_API_KEY}"
    response = requests.get(url)
    return response.json() if response.status_code == 200 else []

def get_stock_quote(symbol):
    """실시간 주가 정보 조회"""
    url = f"https://financialmodelingprep.com/api/v3/quote/{symbol}?apikey={FMP_API_KEY}"
    response = requests.get(url)
    return response.json()[0] if response.status_code == 200 and response.json() else None

def filter_stocks(stocks):
    """주식 필터링"""
    filtered = []
    
    for stock in stocks:
        symbol = stock['symbol']
        quote = get_stock_quote(symbol)
        
        if not quote:
            continue
            
        price = float(quote.get('price', 0))
        volume = int(quote.get('volume', 0))
        yearHigh = float(quote.get('yearHigh', 0))
        marketCap = float(quote.get('marketCap', 0))
        
        if (price >= 10 and 
            volume >= 1000000 and 
            marketCap >= 500000000 and 
            price > yearHigh):  # 52주 신고가 돌파
            
            filtered_stock = {
                'symbol': symbol,
                'price': price,
                'volume': volume,
                'yearHigh': yearHigh,
                'marketCap': marketCap,
                'name': quote.get('name', ''),
                'exchange': quote.get('exchange', '')
            }
            filtered.append(filtered_stock)
            print(f"조건 만족: {symbol} (현재가: ${price:,.2f}, 52주 고가: ${yearHigh:,.2f})")
        
        time.sleep(0.1)  # API 속도 제한 고려
    
    return sorted(filtered, key=lambda x: x['yearHigh'], reverse=True)

def update_airtable(stocks):
    """Airtable 업데이트"""
    airtable = Airtable(AIRTABLE_BASE_ID, TABLE_NAME, AIRTABLE_API_KEY)
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    for stock in stocks:
        record = {
            '티커': stock['symbol'],
            '종목명': stock['name'],
            '현재가': stock['price'],
            '등락률': ((stock['price'] - stock['yearHigh']) / stock['yearHigh']) * 100,
            '거래량': stock['volume'],
            '시가총액': stock['marketCap'],
            '업데이트 시간': current_date,
            '분류': "52주_신고가_돌파",
            '거래소 정보': stock['exchange']
        }
        
        airtable.insert(record)
        print(f"Airtable 데이터 추가: {stock['symbol']}")
        time.sleep(0.2)

def main():
    print("데이터 수집 시작...")
    print("필터링 조건:")
    print("- 현재가 >= $10")
    print("- 거래량 >= 1,000,000주")
    print("- 시가총액 >= $500,000,000")
    print("- 현재가 > 52주 신고가")
    
    stocks = get_tradable_stocks()
    if not stocks:
        print("데이터 수집 실패")
        return
        
    print(f"\n총 {len(stocks)}개 종목 데이터 수집됨")
    
    filtered_stocks = filter_stocks(stocks)
    print(f"\n조건을 만족하는 종목 수: {len(filtered_stocks)}개")
    
    if filtered_stocks:
        update_airtable(filtered_stocks)
    
    print("\n처리 완플렷!")

if __name__ == "__main__":
    main()

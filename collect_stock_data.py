import os
import requests
from datetime import datetime
from airtable import Airtable
import time

FMP_API_KEY = "EApxNJTRwcXOrhy2IUqSeKV0gyH8gans"
AIRTABLE_API_KEY = "patBy8FRWWiG6P99a.a0670e9dd25c84d028c9f708af81d5f1fb164c3adeb1cee067d100075db8b748"
AIRTABLE_BASE_ID = "appAh82iPV3cH6Xx5"
TABLE_NAME = "미국주식 데이터"

def get_all_stocks():
    """모든 거래가능 종목 리스트"""
    url = "https://financialmodelingprep.com/api/v3/available-traded/list"
    params = {'apikey': FMP_API_KEY}
    
    try:
        response = requests.get(url, params=params)
        print(f"전체 종목 데이터 수집: {response.status_code}")
        if response.status_code == 200:
            stocks = response.json()
            # 미국 주식만 필터링 (NASDAQ, NYSE, AMEX)
            us_stocks = [stock for stock in stocks if stock.get('exchangeShortName') in ['NASDAQ', 'NYSE', 'AMEX']]
            print(f"미국 주식 종목 수: {len(us_stocks)}")
            return us_stocks
    except Exception as e:
        print(f"전체 종목 데이터 수집 중 에러: {str(e)}")
    return []

def get_stock_quote(symbol):
    """개별 종목 실시간 시세 조회"""
    url = f"https://financialmodelingprep.com/api/v3/quote/{symbol}"
    params = {'apikey': FMP_API_KEY}
    
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            quotes = response.json()
            if quotes and len(quotes) > 0:
                return quotes[0]
    except Exception as e:
        print(f"{symbol} 시세 조회 중 에러: {str(e)}")
    return None

def get_gainers_data():
    """전일대비등락률상위 데이터"""
    all_stocks = get_all_stocks()
    filtered_stocks = []
    
    print("\n전일대비등락률상위 데이터 처리 중...")
    for stock in all_stocks:
        quote = get_stock_quote(stock.get('symbol'))
        if quote:
            if (quote.get('price', 0) >= 5 and 
                quote.get('volume', 0) >= 1000000 and 
                quote.get('changesPercentage', 0) >= 5):
                filtered_stocks.append(quote)
        time.sleep(0.1)
    
    print(f"조건 만족 종목 수: {len(filtered_stocks)}")
    return sorted(filtered_stocks, key=lambda x: float(x.get('changesPercentage', 0)), reverse=True)

def get_volume_leaders():
    """거래대금상위 데이터"""
    all_stocks = get_all_stocks()
    valid_stocks = []
    
    print("\n거래대금상위 데이터 처리 중...")
    for stock in all_stocks:
        quote = get_stock_quote(stock.get('symbol'))
        if quote and quote.get('price', 0) >= 5:
            quote['trading_value'] = float(quote.get('price', 0)) * float(quote.get('volume', 0))
            valid_stocks.append(quote)
        time.sleep(0.1)
    
    return sorted(valid_stocks, key=lambda x: x.get('trading_value', 0), reverse=True)[:20]

def get_market_cap_leaders():
    """시가총액상위 데이터"""
    all_stocks = get_all_stocks()
    valid_stocks = []
    
    print("\n시가총액상위 데이터 처리 중...")
    for stock in all_stocks:
        quote = get_stock_quote(stock.get('symbol'))
        if quote and quote.get('price', 0) >= 5 and quote.get('volume', 0) >= 1000000:
            valid_stocks.append(quote)
        time.sleep(0.1)
    
    return sorted(valid_stocks, key=lambda x: float(x.get('marketCap', 0)), reverse=True)[:20]

def get_52_week_high():
    """52주 신고가 데이터"""
    all_stocks = get_all_stocks()
    high_stocks = []
    
    print("\n52주 신고가 데이터 처리 중...")
    for stock in all_stocks:
        quote = get_stock_quote(stock.get('symbol'))
        if quote:
            try:
                price = float(quote.get('price', 0))
                year_high = float(quote.get('yearHigh', 0))
                volume = float(quote.get('volume', 0))
                
                if (price >= 5 and 
                    volume >= 1000000 and 
                    year_high > 0 and 
                    price >= year_high * 0.95):
                    high_stocks.append(quote)
            except (TypeError, ValueError) as e:
                continue
        time.sleep(0.1)
    
    print(f"52주 신고가 조건 만족 종목 수: {len(high_stocks)}")
    return sorted(high_stocks, 
                 key=lambda x: (float(x.get('price', 0)) / float(x.get('yearHigh', 1))), 
                 reverse=True)[:20]

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
    print("기준: 현재가 $5↑, 거래량 100만주↑, 현재가가 52주 고가의 95% 이상 (상위 20개)")
    high_52_week = get_52_week_high()
    if high_52_week:
        update_airtable(high_52_week, "52주신고가")
    
    print("\n=== 모든 데이터 처리 완료 ===")

if __name__ == "__main__":
    main()

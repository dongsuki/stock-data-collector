import os
import requests
from datetime import datetime
from airtable import Airtable
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# API 설정
FMP_API_KEY = "EApxNJTRwcXOrhy2IUqSeKV0gyH8gans"
AIRTABLE_API_KEY = "patBy8FRWWiG6P99a.a0670e9dd25c84d028c9f708af81d5f1fb164c3adeb1cee067d100075db8b748"
AIRTABLE_BASE_ID = "appAh82iPV3cH6Xx5"
TABLE_NAME = "미국주식 데이터"

def get_session():
    """Retry를 설정한 세션 생성"""
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session

def get_52week_high_stocks():
    """52주 신고가 근처에 있는 주식 데이터 가져오기"""
    # NYSE, NASDAQ 상장 종목 전체 quote 데이터 가져오기
    url = "https://financialmodelingprep.com/api/v3/stock/list"
    params = {'apikey': FMP_API_KEY}
    
    session = get_session()
    response = session.get(url, params=params)
    
    if response.status_code != 200:
        raise Exception(f"API 호출 실패: {response.status_code}")
    
    stocks = response.json()
    
    # 조건에 맞는 종목 필터링
    # 종목 리스트를 50개씩 나눠서 quote 데이터 가져오기
    filtered_stocks = []
    nyse_nasdaq_stocks = [stock for stock in stocks if stock.get('exchange') in ['NYSE', 'NASDAQ']]
    
    for i in range(0, len(nyse_nasdaq_stocks), 50):
        chunk = nyse_nasdaq_stocks[i:i+50]
        symbols = ','.join(stock['symbol'] for stock in chunk)
        
        # Quote 데이터 가져오기
        quote_url = f"https://financialmodelingprep.com/api/v3/quote/{symbols}"
        quote_response = session.get(quote_url, params=params)
        
        if quote_response.status_code == 200:
            quotes = quote_response.json()
            for quote in quotes:
                if (quote.get('price', 0) >= 5 and
                    quote.get('volume', 0) >= 1000000 and
                    quote.get('marketCap', 0) >= 500000000 and
                    quote.get('yearHigh', 0) > 0):
                    
                    high_ratio = (quote['price'] / quote['yearHigh']) * 100
                    if high_ratio >= 95:
                        quote['highRatio'] = high_ratio
                        filtered_stocks.append(quote)
                        print(f"조건 충족: {quote['symbol']} - 현재가: ${quote['price']} - 52주 신고가: ${quote['yearHigh']} - 비율: {high_ratio:.2f}%")
    
    return sorted(filtered_stocks, key=lambda x: x['highRatio'], reverse=True)[:20]

def update_airtable(stock_data):
    """Airtable에 데이터 추가"""
    airtable = Airtable(AIRTABLE_BASE_ID, TABLE_NAME, AIRTABLE_API_KEY)
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    records = []
    for stock in stock_data:
        record = {
            '티커': stock.get('symbol', ''),
            '종목명': stock.get('name', ''),
            '현재가': float(stock.get('price', 0)),
            '52주 신고가': float(stock.get('yearHigh', 0)),
            '신고가 비율(%)': round(stock.get('highRatio', 0), 2),
            '거래량': int(stock.get('volume', 0)),
            '시가총액': float(stock.get('marketCap', 0)),
            '업데이트 시간': current_date,
            '분류': '52주신고가상위',
            '거래소 정보': stock.get('exchange', '')
        }
        records.append(record)
    
    try:
        airtable.batch_insert(records)
        print(f"{len(records)}개의 데이터가 Airtable에 추가되었습니다.")
    except Exception as e:
        print(f"Airtable 업로드 중 오류 발생: {str(e)}")

def main():
    print("데이터 수집 시작...")
    print("필터링 조건:")
    print("- 현재가 >= $5")
    print("- 거래량 >= 1,000,000주")
    print("- 시가총액 >= $500,000,000")
    print("- 현재가가 52주 신고가의 95% 이상")
    
    try:
        stocks = get_52week_high_stocks()
        if not stocks:
            print("조건에 맞는 종목이 없습니다.")
            return
            
        print(f"\n조건을 만족하는 종목 수: {len(stocks)}개")
        update_airtable(stocks)
        print("\n모든 데이터 처리 완료!")
        
    except Exception as e:
        print(f"프로그램 실행 중 오류 발생: {str(e)}")

if __name__ == "__main__":
    main()

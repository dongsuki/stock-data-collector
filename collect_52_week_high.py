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

def get_bulk_quotes(session, symbols):
    """여러 주식의 정보를 한 번에 가져오기"""
    # 심볼들을 쉼표로 구분된 문자열로 변환
    symbols_str = ','.join(symbols)
    url = f"https://financialmodelingprep.com/api/v3/quote/{symbols_str}"
    params = {'apikey': FMP_API_KEY}
    
    try:
        response = session.get(url, params=params)
        if response.status_code == 200:
            # 응답 데이터를 심볼을 키로 하는 딕셔너리로 변환
            return {quote['symbol']: quote for quote in response.json()}
    except Exception as e:
        print(f"Bulk Quote API 호출 실패: {str(e)}")
    return {}

def get_52week_high_stocks():
    """52주 신고가 근처에 있는 주식 데이터 가져오기"""
    url = "https://financialmodelingprep.com/api/v3/stock-screener"
    params = {
        'marketCapMoreThan': 500000000,
        'priceMoreThan': 5,
        'volumeMoreThan': 1000000,
        'apikey': FMP_API_KEY
    }
    
    session = get_session()
    response = session.get(url, params=params)
    
    if response.status_code != 200:
        raise Exception(f"API 호출 실패: {response.status_code}")
    
    stocks = response.json()
    print(f"응답된 주식 데이터 개수: {len(stocks)}")
    
    # 심볼 목록 추출
    symbols = [stock['symbol'] for stock in stocks if stock.get('symbol')]
    
    # 심볼을 100개씩 나누어 bulk API 호출
    filtered_stocks = []
    for i in range(0, len(symbols), 100):
        chunk_symbols = symbols[i:i+100]
        quotes = get_bulk_quotes(session, chunk_symbols)
        
        for stock in stocks:
            symbol = stock.get('symbol')
            if not symbol or symbol not in quotes:
                continue
                
            quote_data = quotes[symbol]
            stock.update(quote_data)
            
            print(f"검사 중: {symbol} - 현재가: {stock.get('price')} - 52주 신고가: {stock.get('yearHigh')} - 거래량: {stock.get('volume')} - 시가총액: {stock.get('marketCap')}")
            
            if not stock.get('price') or not stock.get('yearHigh'):
                print(f"필수 필드 누락: {symbol}")
                continue
                
            high_ratio = (stock['price'] / stock['yearHigh']) * 100
            if stock['price'] >= stock['yearHigh'] * 0.95:
                print(f"조건 충족: {symbol} - 현재가: {stock['price']} - 52주 신고가: {stock['yearHigh']} - 비율: {high_ratio:.2f}%")
                stock['highRatio'] = high_ratio
                filtered_stocks.append(stock)
    
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

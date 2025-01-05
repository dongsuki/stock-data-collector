import requests
from datetime import datetime
from airtable import Airtable
import time

# API 설정
FMP_API_KEY = "EApxNJTRwcXOrhy2IUqSeKV0gyH8gans"
AIRTABLE_API_KEY = "patBy8FRWWiG6P99a.a0670e9dd25c84d028c9f708af81d5f1fb164c3adeb1cee067d100075db8b748"
AIRTABLE_BASE_ID = "appAh82iPV3cH6Xx5"
TABLE_NAME = "미국주식 데이터"

def get_tradable_companies():
    """거래 가능한 기업 목록 가져오기"""
    url = "https://financialmodelingprep.com/api/v3/available-traded/list"
    params = {'apikey': FMP_API_KEY}
    
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            # NYSE와 NASDAQ 상장 기업만 필터링
            companies = [comp for comp in response.json() 
                       if comp.get('exchangeShortName') in ['NYSE', 'NASDAQ']]
            return companies
        else:
            print(f"거래 가능 기업 목록 요청 실패: {response.status_code}")
            return []
    except Exception as e:
        print(f"거래 가능 기업 목록 조회 중 에러: {str(e)}")
        return []

def get_market_cap_data(companies):
    """시가총액 데이터 가져오기"""
    market_cap_data = []
    
    for company in companies:
        ticker = company['symbol']
        url = f"https://financialmodelingprep.com/api/v3/market-capitalization/{ticker}"
        params = {'apikey': FMP_API_KEY}
        
        try:
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()[0]
                market_cap_data.append({
                    'ticker': ticker,
                    'name': company.get('name', ''),
                    'exchange': company.get('exchangeShortName', ''),
                    'market_cap': data.get('marketCap', 0),
                    'date': data.get('date', '')
                })
            time.sleep(0.1)  # API 제한 고려
        except Exception as e:
            print(f"시가총액 데이터 조회 중 에러 ({ticker}): {str(e)}")
            continue
    
    # 시가총액 기준 정렬 후 상위 20개 선택
    return sorted(market_cap_data, key=lambda x: x.get('market_cap', 0), reverse=True)[:20]

def get_current_quotes(companies):
    """현재 시세 데이터 가져오기"""
    tickers = [company['ticker'] for company in companies]
    ticker_string = ','.join(tickers)
    
    url = f"https://financialmodelingprep.com/api/v3/quote/{ticker_string}"
    params = {'apikey': FMP_API_KEY}
    
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            return {quote['symbol']: quote for quote in response.json()}
        else:
            print(f"시세 데이터 요청 실패: {response.status_code}")
            return {}
    except Exception as e:
        print(f"시세 데이터 조회 중 에러: {str(e)}")
        return {}

def update_airtable(companies, quotes):
    """Airtable에 데이터 업데이트"""
    airtable = Airtable(AIRTABLE_BASE_ID, TABLE_NAME, AIRTABLE_API_KEY)
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    for company in companies:
        try:
            ticker = company['ticker']
            quote = quotes.get(ticker, {})
            
            record = {
                '티커': ticker,
                '종목명': company['name'],
                '현재가': float(quote.get('price', 0)),
                '등락률': float(quote.get('changesPercentage', 0)),
                '거래량': int(quote.get('volume', 0)),
                '거래대금': float(quote.get('price', 0)) * float(quote.get('volume', 0)),
                '시가총액': float(company['market_cap']),
                '거래소 정보': company['exchange'],
                '업데이트 시간': current_date,
                '분류': "시가총액 상위"
            }
            
            airtable.insert(record)
            print(f"데이터 추가 완료: {ticker}")
            time.sleep(0.2)  # Airtable API 제한 고려
            
        except Exception as e:
            print(f"Airtable 업데이트 중 에러 ({ticker}): {str(e)}")

def main():
    print("시가총액 상위 20개 기업 데이터 수집 시작...")
    
    # 1. 거래 가능한 기업 목록 가져오기
    companies = get_tradable_companies()
    if not companies:
        print("거래 가능 기업 목록 조회 실패")
        return
    print(f"거래 가능 기업 {len(companies)}개 조회 완료")
    
    # 2. 시가총액 데이터 가져오기
    market_cap_leaders = get_market_cap_data(companies)
    if not market_cap_leaders:
        print("시가총액 데이터 조회 실패")
        return
    print(f"시가총액 상위 {len(market_cap_leaders)}개 기업 선정 완료")
    
    # 3. 현재 시세 데이터 가져오기
    quotes = get_current_quotes(market_cap_leaders)
    if not quotes:
        print("시세 데이터 조회 실패")
        return
    print("현재 시세 데이터 조회 완료")
    
    # 4. Airtable 업데이트
    print("\nAirtable 업데이트 시작...")
    update_airtable(market_cap_leaders, quotes)
    
    print("\n모든 작업 완료!")

if __name__ == "__main__":
    main()

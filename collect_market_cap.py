import requests
from datetime import datetime
from airtable import Airtable
import time

# API 설정
FMP_API_KEY = "EApxNJTRwcXOrhy2IUqSeKV0gyH8gans"
AIRTABLE_API_KEY = "patBy8FRWWiG6P99a.a0670e9dd25c84d028c9f708af81d5f1fb164c3adeb1cee067d100075db8b748"
AIRTABLE_BASE_ID = "appAh82iPV3cH6Xx5"
TABLE_NAME = "미국주식 데이터"

def get_top_market_cap_companies():
    """시가총액 상위 기업 데이터 가져오기"""
    # stock screener를 사용하여 한 번의 API 호출로 필요한 데이터 조회
    url = "https://financialmodelingprep.com/api/v3/stock-screener"
    params = {
        'apikey': FMP_API_KEY,
        'marketCapMoreThan': 1000000000,  # 10억 달러 이상
        'exchange': 'NYSE,NASDAQ',
        'limit': 1000  # 충분한 데이터 풀
    }
    
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            companies = response.json()
            # 시가총액 기준 정렬 후 상위 20개 선택
            top_companies = sorted(companies, 
                                 key=lambda x: float(x.get('marketCap', 0)), 
                                 reverse=True)[:20]
            
            # 선택된 회사들의 ticker 목록
            tickers = [company['symbol'] for company in top_companies]
            
            # 현재 가격 정보 일괄 조회
            quote_url = f"https://financialmodelingprep.com/api/v3/quote/{','.join(tickers)}"
            quote_response = requests.get(quote_url, params={'apikey': FMP_API_KEY})
            
            if quote_response.status_code == 200:
                quotes = {quote['symbol']: quote for quote in quote_response.json()}
                
                # 최종 데이터 구성
                final_data = []
                for company in top_companies:
                    ticker = company['symbol']
                    quote = quotes.get(ticker, {})
                    
                    final_data.append({
                        'ticker': ticker,
                        'name': company.get('companyName', ''),
                        'price': quote.get('price', 0),
                        'change_percentage': quote.get('changesPercentage', 0),
                        'volume': quote.get('volume', 0),
                        'market_cap': company.get('marketCap', 0),
                        'exchange': company.get('exchange', ''),
                        'trading_value': float(quote.get('price', 0)) * float(quote.get('volume', 0))
                    })
                
                return final_data
            
        print(f"API 요청 실패: {response.status_code}")
        return []
    except Exception as e:
        print(f"데이터 수집 중 에러 발생: {str(e)}")
        return []

def update_airtable(companies):
    """Airtable에 데이터 업데이트"""
    airtable = Airtable(AIRTABLE_BASE_ID, TABLE_NAME, AIRTABLE_API_KEY)
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    for company in companies:
        try:
            record = {
                '티커': company['ticker'],
                '종목명': company['name'],
                '현재가': float(company['price']),
                '등락률': float(company['change_percentage']),
                '거래량': int(company['volume']),
                '거래대금': float(company['trading_value']),
                '시가총액': float(company['market_cap']),
                '거래소 정보': company['exchange'],
                '업데이트 시간': current_date,
                '분류': "시가총액 상위"
            }
            
            airtable.insert(record)
            print(f"데이터 추가 완료: {company['ticker']}")
            time.sleep(0.2)  # Airtable API 제한 고려
            
        except Exception as e:
            print(f"Airtable 업데이트 중 에러 ({company['ticker']}): {str(e)}")

def main():
    print("시가총액 상위 20개 기업 데이터 수집 시작...")
    
    # 데이터 수집 및 처리
    companies = get_top_market_cap_companies()
    if not companies:
        print("데이터 수집 실패")
        return
    
    print(f"\n{len(companies)}개 기업 데이터 수집 완료")
    
    # Airtable 업데이트
    print("\nAirtable 업데이트 시작...")
    update_airtable(companies)
    
    print("\n모든 작업 완료!")

if __name__ == "__main__":
    main()

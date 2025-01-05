import os
import requests
from datetime import datetime
from airtable import Airtable
import time

# API 설정
FMP_API_KEY = "EApxNJTRwcXOrhy2IUqSeKV0gyH8gans"
AIRTABLE_API_KEY = "patBy8FRWWiG6P99a.a0670e9dd25c84d028c9f708af81d5f1fb164c3adeb1cee067d100075db8b748"
AIRTABLE_BASE_ID = "appAh82iPV3cH6Xx5"
TABLE_NAME = "미국주식 데이터"

def get_52week_high_stocks():
    """52주 신고가 근처에 있는 주식 데이터 가져오기"""
    url = "https://financialmodelingprep.com/api/v3/stock-screener"
    
    params = {
        'apikey': FMP_API_KEY,
        'volumeMoreThan': 1000000,  # 거래량 100만주 이상
        'priceMoreThan': 5,  # 주가 5달러 이상
        'marketCapMoreThan': 500000000,  # 시가총액 5억달러 이상
        'exchange': 'NYSE,NASDAQ',  # NYSE와 NASDAQ 상장 종목만
        'limit': 1000  # 충분한 데이터를 가져오기 위해 큰 값 설정
    }
    
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            stocks = response.json()
            # 52주 고가와 현재가 비교하여 필터링 및 정렬
            filtered_stocks = []
            for stock in stocks:
                if stock['price'] > 0 and stock['yearHigh'] > 0:
                    # 현재가가 52주 고가의 95% 이상인 종목만 선택
                    if stock['price'] >= stock['yearHigh'] * 0.95:
                        # 52주 고가 대비 현재가 비율 계산
                        stock['highRatio'] = (stock['price'] / stock['yearHigh']) * 100
                        filtered_stocks.append(stock)
            
            # 52주 고가 비율로 정렬하고 상위 20개만 선택
            sorted_stocks = sorted(filtered_stocks, 
                                 key=lambda x: x['highRatio'], 
                                 reverse=True)[:20]
            
            return sorted_stocks
    except Exception as e:
        print(f"데이터 수집 중 에러 발생: {str(e)}")
        return []

def update_airtable(stock_data):
    """Airtable에 데이터 추가"""
    airtable = Airtable(AIRTABLE_BASE_ID, TABLE_NAME, AIRTABLE_API_KEY)
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    for stock in stock_data:
        try:
            record = {
                '티커': stock.get('symbol', ''),
                '종목명': stock.get('companyName', ''),
                '현재가': float(stock.get('price', 0)),
                '52주 고가': float(stock.get('yearHigh', 0)),
                '52주 고가비율': round(stock.get('highRatio', 0), 2),
                '거래량': int(stock.get('volume', 0)),
                '시가총액': float(stock.get('marketCap', 0)),
                '업데이트 시간': current_date,
                '분류': '52주신고가상위',
                '거래소 정보': stock.get('exchange', '')
            }
            
            if not record['티커']:
                print(f"필수 필드 누락: {stock}")
                continue
            
            airtable.insert(record)
            print(f"새 데이터 추가 완료: {record['티커']}")
            
            time.sleep(0.2)
            
        except Exception as e:
            print(f"레코드 처리 중 에러 발생 ({stock.get('symbol', 'Unknown')}): {str(e)}")

def main():
    print("데이터 수집 시작...")
    print("필터링 조건:")
    print("- 현재가 >= $5")
    print("- 거래량 >= 1,000,000주")
    print("- 시가총액 >= $500,000,000")
    print("- 현재가가 52주 고가의 95% 이상")
    
    stocks = get_52week_high_stocks()
    if not stocks:
        print("데이터 수집 실패")
        return
        
    print(f"\n조건을 만족하는 종목 수: {len(stocks)}개")
    
    if stocks:
        update_airtable(stocks)
    
    print("\n모든 데이터 처리 완료!")

if __name__ == "__main__":
    main()

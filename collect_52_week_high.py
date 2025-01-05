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
    # 먼저 모든 주식 정보를 가져옵니다
    url = "https://financialmodelingprep.com/api/v3/stock/list"
    
    params = {
        'apikey': FMP_API_KEY
    }
    
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            all_stocks = response.json()
            
            # 조건에 맞는 주식만 필터링
            filtered_stocks = []
            
            for stock in all_stocks:
                if not stock.get('symbol'):  # symbol이 없는 경우 스킵
                    continue
                    
                # 각 주식의 상세 정보를 가져옵니다
                quote_url = f"https://financialmodelingprep.com/api/v3/quote/{stock['symbol']}"
                quote_response = requests.get(quote_url, params=params)
                
                if quote_response.status_code == 200:
                    quote_data = quote_response.json()
                    if not quote_data:  # 데이터가 비어있으면 스킵
                        continue
                    
                    quote = quote_data[0]
                    
                    # 필터링 조건 적용
                    if (quote.get('price', 0) >= 5 and
                        quote.get('volume', 0) >= 1000000 and
                        quote.get('marketCap', 0) >= 500000000 and
                        quote.get('yearHigh', 0) > 0):
                        
                        # 현재가가 52주 고가의 95% 이상인 경우
                        if quote['price'] >= quote['yearHigh'] * 0.95:
                            # 52주 고가 대비 현재가 비율 계산
                            quote['highRatio'] = (quote['price'] / quote['yearHigh']) * 100
                            filtered_stocks.append(quote)
                            print(f"조건 충족: {quote['symbol']} - 현재가: ${quote['price']}, 52주 고가: ${quote['yearHigh']}")
                
                time.sleep(0.2)  # API 속도 제한 준수
            
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
                '종목명': stock.get('name', ''),
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

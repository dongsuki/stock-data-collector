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

def get_all_quotes():
    """모든 주식 데이터 한 번에 가져오기"""
    print("데이터 수집 시작...")
    
    # 먼저 tradable stocks 목록을 가져옵니다
    base_url = "https://financialmodelingprep.com/api/v3/available-traded/list"
    params = {'apikey': FMP_API_KEY}
    
    try:
        response = requests.get(base_url, params=params, timeout=30)
        if response.status_code == 200:
            stocks = response.json()
            print(f"거래 가능 종목 수: {len(stocks)}개")
            
            # 샘플 데이터 출력
            if stocks:
                print("\n샘플 데이터:")
                print(stocks[0])
            
            # 이제 각 종목의 상세 정보를 가져옵니다
            detailed_stocks = []
            symbols = [stock['symbol'] for stock in stocks[:100]]  # 테스트를 위해 100개만
            
            batch_url = f"https://financialmodelingprep.com/api/v3/quote/{','.join(symbols)}"
            batch_response = requests.get(batch_url, params=params, timeout=30)
            
            if batch_response.status_code == 200:
                detailed_stocks = batch_response.json()
                print(f"\n상세 데이터 수집 완료. 샘플:")
                if detailed_stocks:
                    print(detailed_stocks[0])
                return detailed_stocks
            else:
                print(f"상세 데이터 요청 실패: {batch_response.status_code}")
                return []
        else:
            print(f"API 요청 실패: {response.status_code}")
            return []
    except Exception as e:
        print(f"데이터 수집 중 에러 발생: {str(e)}")
        return []

def filter_stocks(stocks):
    """주식 필터링"""
    print("\n필터링 시작...")
    print(f"필터링 전 종목 수: {len(stocks)}")
    filtered = []
    
    for stock in stocks:
        try:
            price = float(stock.get('price', 0))
            volume = float(stock.get('volume', 0))
            yearHigh = float(stock.get('yearHigh', 0))
            marketCap = float(stock.get('marketCap', 0))
            
            print(f"\n종목: {stock.get('symbol')}")
            print(f"가격: ${price:.2f}")
            print(f"거래량: {volume:,.0f}")
            print(f"시가총액: ${marketCap:,.2f}")
            print(f"52주 고가: ${yearHigh:.2f}")
            
            if price >= 10:
                print("조건1 만족: 가격 >= $10")
                if volume >= 1000000:
                    print("조건2 만족: 거래량 >= 1,000,000")
                    if marketCap >= 500000000:
                        print("조건3 만족: 시가총액 >= $500,000,000")
                        if price > yearHigh and yearHigh > 0:
                            print("조건4 만족: 현재가 > 52주 고가")
                            filtered.append(stock)
                            print(f">>> {stock.get('symbol')} 모든 조건 만족!")
            
        except Exception as e:
            print(f"처리 중 에러 발생 ({stock.get('symbol', 'Unknown')}): {str(e)}")
            continue
    
    print(f"\n필터링 결과: {len(filtered)}개 종목 조건 만족")
    return filtered

def main():
    print("프로그램 시작...")
    print("필터링 조건:")
    print("- 현재가 >= $10")
    print("- 거래량 >= 1,000,000주")
    print("- 시가총액 >= $500,000,000")
    print("- 현재가 > 52주 신고가")
    
    start_time = time.time()
    stocks = get_all_quotes()
    
    if not stocks:
        print("데이터 수집 실패")
        return
        
    filtered_stocks = filter_stocks(stocks)
    
    end_time = time.time()
    print(f"\n처리 완료! (소요시간: {end_time - start_time:.2f}초)")

if __name__ == "__main__":
    main()

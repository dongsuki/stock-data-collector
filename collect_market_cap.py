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

def safe_float(value, default=0.0):
    try:
        if value is None:
            return default
        return float(value)
    except (ValueError, TypeError):
        return default

def get_all_quotes():
    """모든 주식 데이터 한 번에 가져오기"""
    print("데이터 수집 시작...")
    
    # 미국 주식 전체 시세 데이터를 가져옵니다
    url = f"https://financialmodelingprep.com/api/v3/quotes/nasdaq?apikey={FMP_API_KEY}"
    
    try:
        stocks = []
        
        # NASDAQ 데이터
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            stocks.extend(response.json())
            
        # NYSE 데이터    
        url = f"https://financialmodelingprep.com/api/v3/quotes/nyse?apikey={FMP_API_KEY}"
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            stocks.extend(response.json())
            
        print(f"\n수집된 종목 수: {len(stocks)}개")
        
        if stocks:
            # 첫 번째 종목의 모든 필드 출력
            print("\n첫 번째 종목 데이터 구조:")
            first_stock = stocks[0]
            for key, value in first_stock.items():
                print(f"{key}: {value}")
                
        return stocks
    except Exception as e:
        print(f"데이터 수집 중 에러 발생: {str(e)}")
        return []

def filter_stocks(stocks):
    """주식 필터링"""
    print("\n필터링 시작...")
    print(f"필터링 전 종목 수: {len(stocks)}개")
    filtered = []
    failed = 0
    
    for stock in stocks:
        try:
            price = safe_float(stock.get('price'))
            volume = safe_float(stock.get('volume'))
            yearHigh = safe_float(stock.get('yearHigh'))
            marketCap = safe_float(stock.get('marketCap'))
            
            # 데이터 유효성 검사
            if price == 0 or yearHigh == 0:
                failed += 1
                continue

            # 52주 고가 대비 현재가 비율 계산
            price_to_high_ratio = (price / yearHigh) * 100
            
            if (price >= 10 and 
                volume >= 1000000 and 
                marketCap >= 500000000 and 
                price_to_high_ratio >= 95):
                
                filtered.append({
                    'symbol': stock.get('symbol'),
                    'price': price,
                    'volume': volume,
                    'yearHigh': yearHigh,
                    'marketCap': marketCap,
                    'name': stock.get('name', ''),
                    'exchange': stock.get('exchange', ''),
                    'price_to_high_ratio': price_to_high_ratio
                })
                
                print(f"\n조건 만족: {stock.get('symbol')}")
                print(f"현재가: ${price:,.2f}")
                print(f"거래량: {volume:,.0f}")
                print(f"52주 고가: ${yearHigh:,.2f}")
                print(f"시가총액: ${marketCap:,.2f}")
                print(f"52주 고가 대비: {price_to_high_ratio:.1f}%")
            
        except Exception as e:
            failed += 1
            print(f"처리 중 에러 발생 ({stock.get('symbol', 'Unknown')}): {str(e)}")
            continue
    
    print(f"\n데이터 처리 실패: {failed}개 종목")
    filtered = sorted(filtered, key=lambda x: x['price_to_high_ratio'], reverse=True)
    print(f"조건 만족: {len(filtered)}개 종목")
    return filtered

def update_airtable(stocks):
    """Airtable 업데이트"""
    print("\nAirtable 업데이트 시작...")
    airtable = Airtable(AIRTABLE_BASE_ID, TABLE_NAME, AIRTABLE_API_KEY)
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    for i, stock in enumerate(stocks, 1):
        try:
            record = {
                '티커': stock['symbol'],
                '종목명': stock['name'],
                '현재가': stock['price'],
                '등락률': stock['price_to_high_ratio'] - 100,  # 52주 고가 대비 등락률
                '거래량': stock['volume'],
                '시가총액': stock['marketCap'],
                '업데이트 시간': current_date,
                '분류': "52주_신고가_근접",
                '거래소 정보': stock['exchange']
            }
            
            airtable.insert(record)
            print(f"[{i}/{len(stocks)}] {stock['symbol']} 추가 완료")
            time.sleep(0.2)
        except Exception as e:
            print(f"Airtable 업데이트 실패 ({stock['symbol']}): {str(e)}")

def main():
    print("프로그램 시작...")
    print("필터링 조건:")
    print("- 현재가 >= $10")
    print("- 거래량 >= 1,000,000주")
    print("- 시가총액 >= $500,000,000")
    print("- 현재가가 52주 고가의 95% 이상")
    print("- 미국 거래소 상장 종목만")
    
    start_time = time.time()
    stocks = get_all_quotes()
    
    if not stocks:
        print("데이터 수집 실패")
        return
    
    filtered_stocks = filter_stocks(stocks)
    
    if filtered_stocks:
        update_airtable(filtered_stocks)
    
    end_time = time.time()
    print(f"\n처리 완료! (소요시간: {end_time - start_time:.2f}초)")

if __name__ == "__main__":
    main()

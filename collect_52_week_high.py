import os
import requests
from datetime import datetime
from airtable import Airtable
import time

FMP_API_KEY = "EApxNJTRwcXOrhy2IUqSeKV0gyH8gans"
AIRTABLE_API_KEY = "patBy8FRWWiG6P99a.a0670e9dd25c84d028c9f708af81d5f1fb164c3adeb1cee067d100075db8b748"
AIRTABLE_BASE_ID = "appAh82iPV3cH6Xx5"
TABLE_NAME = "미국주식 데이터"


def safe_float(value, default=0.0):
    """안전하게 float로 변환"""
    try:
        if value is None:
            return default
        return float(value)
    except (ValueError, TypeError):
        return default

def get_all_quotes():
    """모든 주식 데이터 한 번에 가져오기"""
    print("데이터 수집 시작...")
    url = f"https://financialmodelingprep.com/api/v3/quotes/index?apikey={FMP_API_KEY}"
    
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            data = response.json()
            print(f"총 {len(data)}개 종목 데이터 수집 완료")
            return data
        else:
            print(f"API 요청 실패: {response.status_code}")
            return []
    except requests.Timeout:
        print("API 요청 시간 초과")
        return []
    except Exception as e:
        print(f"데이터 수집 중 에러 발생: {str(e)}")
        return []

def filter_stocks(stocks):
    """주식 필터링"""
    print("필터링 시작...")
    filtered = []
    count = 0
    
    for stock in stocks:
        try:
            price = safe_float(stock.get('price'))
            volume = safe_float(stock.get('volume'))
            yearHigh = safe_float(stock.get('yearHigh'))
            marketCap = safe_float(stock.get('marketCap'))
            
            if (price >= 10 and 
                volume >= 1000000 and 
                marketCap >= 500000000 and 
                price > yearHigh > 0):  # yearHigh가 0보다 커야 함
                
                filtered.append({
                    'symbol': stock.get('symbol'),
                    'price': price,
                    'volume': volume,
                    'yearHigh': yearHigh,
                    'marketCap': marketCap,
                    'name': stock.get('name', ''),
                    'exchange': stock.get('exchange', '')
                })
                
                count += 1
                if count % 10 == 0:
                    print(f"조건 만족 종목 수: {count}개")
                print(f"조건 만족: {stock.get('symbol')} (현재가: ${price:,.2f}, 52주고가: ${yearHigh:,.2f})")
        except Exception as e:
            print(f"종목 처리 중 에러 발생 ({stock.get('symbol', 'Unknown')}): {str(e)}")
            continue
    
    filtered = sorted(filtered, key=lambda x: x['yearHigh'], reverse=True)
    print(f"\n총 {len(filtered)}개 종목이 조건을 만족")
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
                '등락률': ((stock['price'] - stock['yearHigh']) / stock['yearHigh']) * 100 if stock['yearHigh'] != 0 else 0,
                '거래량': stock['volume'],
                '시가총액': stock['marketCap'],
                '업데이트 시간': current_date,
                '분류': "52주_신고가_돌파",
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
    print("- 현재가 > 52주 신고가")
    
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

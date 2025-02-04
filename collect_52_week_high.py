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
    """안전하게 float로 변환"""
    try:
        if value is None:
            return default
        return float(value)
    except (ValueError, TypeError):
        return default

def is_us_exchange(exchange):
    """미국 거래소인지 확인"""
    us_exchanges = {'NYSE', 'NASDAQ', 'AMEX'}
    return exchange in us_exchanges

def get_all_quotes():
    """미국 주식 데이터 가져오기"""
    print("데이터 수집 시작...")
    url = f"https://financialmodelingprep.com/api/v3/stock/list?apikey={FMP_API_KEY}"
    
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            # 미국 거래소 종목만 필터링
            stocks = [stock for stock in response.json() if is_us_exchange(stock.get('exchange', ''))]
            print(f"미국 상장 종목 수: {len(stocks)}개")
            
            # 종목들의 상세 정보 수집
            symbols = [stock['symbol'] for stock in stocks]
            batch_size = 100
            detailed_stocks = []
            
            for i in range(0, len(symbols), batch_size):
                batch = symbols[i:i + batch_size]
                batch_url = f"https://financialmodelingprep.com/api/v3/quote/{','.join(batch)}?apikey={FMP_API_KEY}"
                batch_response = requests.get(batch_url, timeout=30)
                
                if batch_response.status_code == 200:
                    detailed_stocks.extend(batch_response.json())
                    print(f"처리 중... {i + len(batch)}/{len(symbols)} 종목")
                time.sleep(0.1)  # API 속도 제한 고려
                
            return detailed_stocks
        else:
            print(f"API 요청 실패: {response.status_code}")
            return []
    except Exception as e:
        print(f"데이터 수집 중 에러 발생: {str(e)}")
        return []

def filter_stocks(stocks):
    """주식 필터링"""
    print("\n필터링 시작...")
    filtered = []
    
    for stock in stocks:
        try:
            price = safe_float(stock.get('price'))
            volume = safe_float(stock.get('volume'))
            yearHigh = safe_float(stock.get('yearHigh'))
            marketCap = safe_float(stock.get('marketCap'))
            
            # 52주 고가 대비 현재가 비율 계산
            if yearHigh > 0:
                price_to_high_ratio = (price / yearHigh) * 100
            else:
                continue
            
            # 조건 체크
            if (price >= 10 and 
                volume >= 1000000 and 
                marketCap >= 500000000 and 
                price_to_high_ratio >= 95):  # 52주 고가의 95% 이상
                
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
                
                print(f"조건 만족: {stock.get('symbol')} (현재가: ${price:,.2f}, "
                      f"52주고가: ${yearHigh:,.2f}, 비율: {price_to_high_ratio:.1f}%)")
                
        except Exception as e:
            print(f"종목 처리 중 에러 발생 ({stock.get('symbol', 'Unknown')}): {str(e)}")
            continue
    
    filtered = sorted(filtered, key=lambda x: x['price_to_high_ratio'], reverse=True)
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

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
    
    # 실시간 주가 데이터를 한 번에 가져옵니다
    url = f"https://financialmodelingprep.com/api/v3/quotes/index?apikey={FMP_API_KEY}"
    
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            # 미국 거래소 종목만 필터링
            stocks = [stock for stock in response.json() 
                     if is_us_exchange(stock.get('exchange', ''))]
            
            print(f"미국 상장 종목 수: {len(stocks)}개")
            
            # 샘플 데이터 출력
            if stocks:
                print("\n첫 번째 종목 데이터 샘플:")
                sample = stocks[0]
                print(f"종목: {sample.get('symbol')}")
                print(f"가격: ${sample.get('price')}")
                print(f"거래량: {sample.get('volume')}")
                print(f"52주 고가: ${sample.get('yearHigh')}")
                print(f"시가총액: ${sample.get('marketCap'):,}")
                print(f"거래소: {sample.get('exchange')}")
            
            return stocks
        else:
            print(f"API 요청 실패: {response.status_code}")
            if response.text:
                print(f"에러 메시지: {response.text}")
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
            price = safe_float(stock.get('price'))
            volume = safe_float(stock.get('volume'))
            yearHigh = safe_float(stock.get('yearHigh'))
            marketCap = safe_float(stock.get('marketCap'))
            
            if not all([price, yearHigh, volume, marketCap]):
                continue
                
            # 52주 고가 대비 현재가 비율 계산
            price_to_high_ratio = (price / yearHigh) * 100
            
            # 조건 체크 및 로깅
            conditions_met = []
            
            if price >= 10:
                conditions_met.append("가격 >= $10")
            if volume >= 1000000:
                conditions_met.append("거래량 >= 1,000,000")
            if marketCap >= 500000000:
                conditions_met.append("시가총액 >= $500M")
            if price_to_high_ratio >= 95:
                conditions_met.append("52주 고가 95% 이상")
                
            # 모든 조건을 만족하면 추가
            if len(conditions_met) == 4:
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
            print(f"\n처리 중 에러 발생 ({stock.get('symbol', 'Unknown')}): {str(e)}")
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

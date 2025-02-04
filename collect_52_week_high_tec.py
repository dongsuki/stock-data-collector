import os
import requests
from datetime import datetime
import time
from airtable import Airtable
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# API 설정
FMP_API_KEY = os.getenv('FMP_API_KEY')
AIRTABLE_API_KEY = os.getenv('AIRTABLE_API_KEY')
AIRTABLE_BASE_ID = os.getenv('AIRTABLE_BASE_ID')
TABLE_NAME = "미국주식 데이터"

# HTTP 세션 설정
session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("http://", adapter)
session.mount("https://", adapter)

def safe_float(value, default=0.0):
    """안전하게 float로 변환"""
    try:
        if value is None:
            return default
        return float(value)
    except (ValueError, TypeError):
        return default

def make_api_request(url, timeout=10):
    """API 요청 함수"""
    try:
        response = session.get(url, timeout=timeout)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ API 요청 실패 ({url}): {str(e)}")
        return None
    except Exception as e:
        print(f"❌ 예상치 못한 오류 ({url}): {str(e)}")
        return None

def get_delisted_stocks():
    """FMP API에서 상장폐지된 종목 목록 가져오기"""
    url = f"https://financialmodelingprep.com/api/v3/delisted-companies?apikey={FMP_API_KEY}"
    data = make_api_request(url)
    if data:
        return {item['symbol'] for item in data}
    return set()

def get_tradable_stocks():
    """현재 거래 가능한 종목 목록 가져오기"""
    url = f"https://financialmodelingprep.com/api/v3/available-traded/list?apikey={FMP_API_KEY}"
    data = make_api_request(url)
    if data:
        return {item['symbol'] for item in data}
    return set()

def get_quotes():
    """미국 주식 데이터 가져오기"""
    print("📡 데이터 수집 시작...")
    all_stocks = []

    # NASDAQ 데이터 수집
    url_nasdaq = f"https://financialmodelingprep.com/api/v3/quotes/nasdaq?apikey={FMP_API_KEY}"
    nasdaq_stocks = make_api_request(url_nasdaq) or []
    print(f"📌 NASDAQ 종목 수집 완료: {len(nasdaq_stocks)}개")
    
    time.sleep(1)  # API 요청 간격
    
    # NYSE 데이터 수집
    url_nyse = f"https://financialmodelingprep.com/api/v3/quotes/nyse?apikey={FMP_API_KEY}"
    nyse_stocks = make_api_request(url_nyse) or []
    print(f"📌 NYSE 종목 수집 완료: {len(nyse_stocks)}개")

    all_stocks = nasdaq_stocks + nyse_stocks
    print(f"✅ 총 수집 종목 수: {len(all_stocks)}개")
    
    return all_stocks

def check_technical_conditions(stock):
    """기술적 지표 조건 확인"""
    try:
        price = safe_float(stock.get('price'))
        ma50 = safe_float(stock.get('priceAvg50'))
        ma150 = safe_float(stock.get('priceAvg150', 0))
        ma200 = safe_float(stock.get('priceAvg200'))
        yearLow = safe_float(stock.get('yearLow'))
        
        if not all([price, ma50, ma150, ma200, yearLow]):  # 필요한 데이터가 없으면 제외
            return False
        
        # 200일 이평선 추세 확인
        url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{stock['symbol']}?apikey={FMP_API_KEY}&serietype=line"
        historical_data = make_api_request(url)
        
        if not historical_data or 'historical' not in historical_data:
            return False
            
        historical_prices = historical_data['historical']
        if len(historical_prices) < 230:  # 충분한 데이터가 없으면 제외
            return False
            
        # 1개월 전 200일 이평선 계산
        previous_ma200 = sum(float(data['close']) for data in historical_prices[30:230]) / 200
        current_ma200 = sum(float(data['close']) for data in historical_prices[0:200]) / 200
        
        # 모든 조건 체크
        conditions = [
            price > ma150 and price > ma200,  # 1. 주가가 150일 및 200일 이평선 위
            ma150 > ma200,  # 2. 150일 이평선이 200일 이평선 위
            current_ma200 > previous_ma200,  # 3. 200일 이평선 1개월 이상 상승 추세
            ma50 > ma150 or ma50 > ma200,  # 4. 50일 이평선이 150일 or 200일 이평선 위
            price > ma50,  # 5. 현 주가가 50일 이평선 위
            price > (yearLow * 1.30),  # 6. 52주 저가 대비 30% 이상
        ]
        
        return all(conditions)
        
    except Exception as e:
        print(f"❌ 기술적 지표 확인 중 오류 ({stock.get('symbol')}): {str(e)}")
        return False

def is_valid_us_stock(stock, delisted_stocks, tradable_stocks):
    """실제 거래 가능한 미국 주식인지 확인"""
    try:
        symbol = stock.get('symbol', '')
        exchange = stock.get('exchange', '')
        type = stock.get('type', '').lower()
        name = stock.get('name', '') or ''
        name = name.lower()
        volume = safe_float(stock.get('volume'))
        price = safe_float(stock.get('price'))

        # ETF 제외
        etf_keywords = ['etf', 'trust', 'fund']
        if 'etf' in type or any(keyword in name for keyword in etf_keywords):
            return False

        # 거래소 확인
        if exchange not in {'NYSE', 'NASDAQ'}:
            return False

        # 상장폐지 및 거래가능 여부 확인
        if symbol in delisted_stocks or symbol not in tradable_stocks:
            return False

        # 특수 증권 키워드 확인
        invalid_keywords = [
            'warrant', 'warrants', 'adr', 'preferred', 'acquisition',
            'right', 'rights', 'merger', 'spac', 'unit', 'notes',
            'bond', 'series', 'class', 'holding', 'holdings', 'partners', 'management'
        ]
        if any(keyword in name for keyword in invalid_keywords):
            return False

        # 거래대금 확인
        min_daily_dollar_volume = 1000000
        if price * volume < min_daily_dollar_volume:
            return False

        return True
        
    except Exception as e:
        print(f"❌ 종목 유효성 검사 중 오류 ({stock.get('symbol')}): {str(e)}")
        return False

def filter_stocks(stocks):
    """주식 필터링"""
    print("\n🔎 필터링 시작...")
    print(f"📌 필터링 전 종목 수: {len(stocks)}개")

    delisted_stocks = get_delisted_stocks()
    time.sleep(1)  # API 요청 간격
    tradable_stocks = get_tradable_stocks()
    
    print(f"✅ 상장폐지 종목 수: {len(delisted_stocks)}개")
    print(f"✅ 현재 거래 가능한 종목 수: {len(tradable_stocks)}개")

    filtered = []
    for stock in stocks:
        try:
            if not is_valid_us_stock(stock, delisted_stocks, tradable_stocks):
                continue

            price = safe_float(stock.get('price'))
            volume = safe_float(stock.get('volume'))
            yearHigh = safe_float(stock.get('yearHigh'))
            marketCap = safe_float(stock.get('marketCap'))

            if not all([price, volume, yearHigh, marketCap]):  # 필수 데이터 확인
                continue

            price_to_high_ratio = (price / yearHigh) * 100
            change_percent = ((price - safe_float(stock.get('previousClose'))) / safe_float(stock.get('previousClose'))) * 100 if safe_float(stock.get('previousClose')) > 0 else 0

            if (price >= 10 and volume >= 1000000 and 
                marketCap >= 500000000 and 
                price_to_high_ratio >= 75):
                
                time.sleep(0.5)  # API 요청 간격
                if check_technical_conditions(stock):
                    filtered.append({
                        'symbol': stock['symbol'],
                        'price': price,
                        'volume': volume,
                        'yearHigh': yearHigh,
                        'marketCap': marketCap,
                        'name': stock['name'],
                        'exchange': stock['exchange'],
                        'price_to_high_ratio': price_to_high_ratio,
                        'change_percent': change_percent
                    })
                    print(f"✅ 조건 만족 종목 추가: {stock['symbol']}")
                    
        except Exception as e:
            print(f"❌ 종목 필터링 중 오류 ({stock.get('symbol')}): {str(e)}")
            continue

    print(f"✅ 최종 조건 만족 종목 수: {len(filtered)}개")
    return sorted(filtered, key=lambda x: x['price_to_high_ratio'], reverse=True)

def update_airtable(stocks):
    """Airtable에 새 레코드 추가"""
    if not stocks:
        print("❌ 업데이트할 데이터가 없습니다.")
        return
        
    print("\n📡 Airtable 업데이트 시작...")
    airtable = Airtable(AIRTABLE_BASE_ID, TABLE_NAME, AIRTABLE_API_KEY)
    current_date = datetime.now().strftime("%Y-%m-%d")

    for stock in stocks:
        try:
            record = {
                '티커': stock['symbol'],
                '종목명': stock['name'],
                '현재가': stock['price'],
                '등락률': stock['change_percent'],
                '거래량': stock['volume'],
                '시가총액': stock['marketCap'],
                '업데이트 시간': current_date,
                '분류': "52주_신고가_근접",
                '거래소 정보': stock['exchange'],
                '신고가 비율(%)': stock['price_to_high_ratio']
            }
            airtable.insert(record)
            print(f"✅ Airtable 업데이트 완료: {stock['symbol']}")
            time.sleep(0.2)  # Airtable API 요청 간격
            
        except Exception as e:
            print(f"❌ Airtable 업데이트 중 오류 ({stock['symbol']}): {str(e)}")
            continue
            
    print("✅ Airtable 전체 업데이트 완료!")

def main():
    try:
        print("🚀 프로그램 시작...")
        stocks = get_quotes()
        if stocks:
            filtered_stocks = filter_stocks(stocks)
            if filtered_stocks:
                update_airtable(filtered_stocks)
        print("✨ 프로그램 종료")
    except Exception as e:
        print(f"❌ 프로그램 실행 중 오류 발생: {str(e)}")

if __name__ == "__main__":
    main()

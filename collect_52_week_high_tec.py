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

class APIRateLimiter:
    def __init__(self, calls_per_minute):
        self.calls_per_minute = calls_per_minute
        self.min_interval = 60.0 / calls_per_minute
        self.last_call_time = 0

    def wait_if_needed(self):
        current_time = time.time()
        elapsed = current_time - self.last_call_time
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_call_time = time.time()

# Rate Limiter 초기화 (300 calls/minute)
rate_limiter = APIRateLimiter(300)

def safe_float(value, default=0.0):
    """안전하게 float로 변환"""
    try:
        if value is None:
            return default
        return float(value)
    except (ValueError, TypeError):
        return default

def get_delisted_stocks():
    """FMP API에서 상장폐지된 종목 목록 가져오기"""
    rate_limiter.wait_if_needed()
    url = f"https://financialmodelingprep.com/api/v3/delisted-companies?apikey={FMP_API_KEY}"
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            return {item['symbol'] for item in response.json()}
        elif response.status_code == 429:
            print("⚠️ API 호출 한도 초과, 잠시 대기 후 재시도...")
            time.sleep(5)
            return get_delisted_stocks()
    except Exception as e:
        print(f"❌ 상장폐지 종목 데이터 가져오기 실패: {str(e)}")
    return set()

def get_tradable_stocks():
    """현재 거래 가능한 종목 목록 가져오기"""
    rate_limiter.wait_if_needed()
    url = f"https://financialmodelingprep.com/api/v3/available-traded/list?apikey={FMP_API_KEY}"
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            return {item['symbol'] for item in response.json()}
        elif response.status_code == 429:
            print("⚠️ API 호출 한도 초과, 잠시 대기 후 재시도...")
            time.sleep(5)
            return get_tradable_stocks()
    except Exception as e:
        print(f"❌ 거래 가능한 종목 데이터 가져오기 실패: {str(e)}")
    return set()

def get_quotes():
    """미국 주식 데이터 가져오기"""
    print("📡 데이터 수집 시작...")

    def fetch_exchange_data(exchange):
        rate_limiter.wait_if_needed()
        try:
            url = f"https://financialmodelingprep.com/api/v3/quotes/{exchange}?apikey={FMP_API_KEY}"
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                data = response.json()
                print(f"📌 {exchange} 종목 수집 완료: {len(data)}개")
                return data
            elif response.status_code == 429:
                print("⚠️ API 호출 한도 초과, 잠시 대기 후 재시도...")
                time.sleep(5)
                return fetch_exchange_data(exchange)
            else:
                print(f"⚠️ {exchange} API 응답 에러: {response.status_code}")
        except Exception as e:
            print(f"❌ {exchange} 데이터 수집 실패: {str(e)}")
        return []

    nasdaq_stocks = fetch_exchange_data("NASDAQ")
    nyse_stocks = fetch_exchange_data("NYSE")
    
    all_stocks = nasdaq_stocks + nyse_stocks
    print(f"✅ 총 수집 종목 수: {len(all_stocks)}개")
    return all_stocks

def get_historical_data(symbol):
    """주가 히스토리 데이터 가져오기"""
    rate_limiter.wait_if_needed()
    url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}?apikey={FMP_API_KEY}"
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 429:
            print(f"⚠️ API 호출 한도 초과, 잠시 대기 후 재시도...")
            time.sleep(5)
            return get_historical_data(symbol)
            
        if response.status_code == 200:
            data = response.json()
            if 'historical' in data:
                return data['historical']
    except Exception as e:
        print(f"❌ {symbol} 주가 히스토리 데이터 가져오기 실패: {str(e)}")
    return None

def calculate_moving_averages(historical_data):
    """이동평균선 계산"""
    if not historical_data or len(historical_data) < 200:  # 최소 200일치 데이터 필요
        return None
        
    # 종가 데이터 추출 (최신 순으로 정렬)
    closes = [float(day['close']) for day in historical_data[:200]]
    
    if not closes:
        return None
        
    ma_data = {
        'MA50': sum(closes[:50]) / 50 if len(closes) >= 50 else None,
        'MA150': sum(closes[:150]) / 150 if len(closes) >= 150 else None,
        'MA200': sum(closes[:200]) / 200 if len(closes) >= 200 else None,
        'MA200_trend': False
    }
    
    # 200일 이평선 추세 확인 (현재값이 한달 전보다 높은지)
    if len(closes) >= 200:
        current_ma200 = ma_data['MA200']
        month_ago_ma200 = sum(closes[20:220]) / 200
        ma_data['MA200_trend'] = current_ma200 > month_ago_ma200
    
    return ma_data

def get_moving_averages(symbol):
    """종목의 이동평균선 데이터 가져오기"""
    historical_data = get_historical_data(symbol)
    if not historical_data:
        print(f"⚠️ {symbol} 주가 데이터 누락")
        return None
        
    ma_data = calculate_moving_averages(historical_data)
    if not ma_data:
        print(f"⚠️ {symbol} 이동평균선 계산 실패")
        return None
        
    return ma_data

def is_valid_us_stock(stock, delisted_stocks, tradable_stocks):
    """실제 거래 가능한 미국 주식인지 확인"""
    symbol = stock.get('symbol', '')
    exchange = stock.get('exchange', '')
    type = stock.get('type', '').lower()
    name = stock.get('name', '') or ''
    name = name.lower()
    volume = safe_float(stock.get('volume'))
    price = safe_float(stock.get('price'))

    etf_keywords = ['etf', 'trust', 'fund']
    if 'etf' in type or any(keyword in name for keyword in etf_keywords):
        return False

    if exchange not in {'NYSE', 'NASDAQ'}:
        return False

    if symbol in delisted_stocks:
        return False

    if symbol not in tradable_stocks:
        return False

    invalid_keywords = [
        'warrant', 'warrants', 'adr', 'preferred', 'acquisition',
        'right', 'rights', 'merger', 'spac', 'unit', 'notes',
        'bond', 'series', 'class', 'holding', 'holdings', 'partners', 'management'
    ]
    if any(keyword in name for keyword in invalid_keywords):
        return False

    min_daily_dollar_volume = 1000000
    if price * volume < min_daily_dollar_volume:
        return False

    return True

def check_technical_conditions(stock, ma_data):
    """기술적 조건 확인"""
    if not all(ma_data.get(key) for key in ['MA50', 'MA150', 'MA200']):
        return False

    current_price = safe_float(stock.get('price'))
    ma50 = safe_float(ma_data.get('MA50'))
    ma150 = safe_float(ma_data.get('MA150'))
    ma200 = safe_float(ma_data.get('MA200'))
    ma200_trend = ma_data.get('MA200_trend')
    year_low = safe_float(stock.get('yearLow'))
    year_high = safe_float(stock.get('yearHigh'))

    return all([
        current_price > ma150,  # 현재가 > 150MA
        current_price > ma200,  # 현재가 > 200MA
        ma150 > ma200,         # 150MA > 200MA
        ma200_trend,           # 200MA 상승추세
        ma50 > ma150,          # 50MA > 150MA
        ma50 > ma200,          # 50MA > 200MA
        current_price > ma50,  # 현재가 > 50MA
        current_price > (year_low * 1.3)  # 저가대비 30% 이상
    ])

def filter_stocks(stocks):
    """주식 필터링"""
    print("\n🔎 필터링 시작...")
    print(f"📌 필터링 전 종목 수: {len(stocks)}개")

    delisted_stocks = get_delisted_stocks()
    tradable_stocks = get_tradable_stocks()
    
    filtered = []
    processed = 0
    for stock in stocks:
        processed += 1
        if processed % 100 == 0:
            print(f"진행 중: {processed}/{len(stocks)} 종목 처리됨")

        if not is_valid_us_stock(stock, delisted_stocks, tradable_stocks):
            continue
            
        price = safe_float(stock.get('price'))
        volume = safe_float(stock.get('volume'))
        yearHigh = safe_float(stock.get('yearHigh'))
        marketCap = safe_float(stock.get('marketCap'))
        
        price_to_high_ratio = (price / yearHigh) * 100 if yearHigh else 0
        
        if not (price >= 10 and volume >= 1000000 and marketCap >= 500000000 and price_to_high_ratio >= 95):
            continue
            
        ma_data = get_moving_averages(stock['symbol'])
        if ma_data is None:
            continue
        
        if check_technical_conditions(stock, ma_data):
            change_percent = ((price - safe_float(stock.get('previousClose'))) / 
                            safe_float(stock.get('previousClose'))) * 100 if safe_float(stock.get('previousClose')) > 0 else 0
            
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
            print(f"✅ 조건 만족 종목 발견: {stock['symbol']}")

    print(f"✅ 모든 조건 만족 종목: {len(filtered)}개")
    return sorted(filtered, key=lambda x: x['price_to_high_ratio'], reverse=True)

def update_airtable(stocks):
    """Airtable에 새 레코드 추가"""
    print("\n📡 Airtable 업데이트 시작...")
    airtable = Airtable(AIRTABLE_BASE_ID, TABLE_NAME, AIRTABLE_API_KEY)
    current_date = datetime.now().strftime("%Y-%m-%d")

    for stock in stocks:
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
    print("✅ Airtable 업데이트 완료!")

def main():
    stocks = get_quotes()
    if stocks:
        filtered_stocks = filter_stocks(stocks)
        if filtered_stocks:
            update_airtable(filtered_stocks)

if __name__ == "__main__":
    main()

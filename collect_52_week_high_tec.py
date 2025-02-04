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

def get_moving_averages(symbol):
    """종목의 이동평균선 데이터 가져오기"""
    ma_data = {
        'MA50': None,
        'MA150': None,
        'MA200': None,
        'MA200_trend': False
    }

    try:
        periods = [50, 150, 200]
        for period in periods:
            rate_limiter.wait_if_needed()
            # URL 수정
            url = f"https://financialmodelingprep.com/api/v3/technical_indicator/1day/{symbol}?period={period}&type=sma&apikey={FMP_API_KEY}"
            response = requests.get(url, timeout=30)
            
            if response.status_code == 429:
                print(f"⚠️ API 호출 한도 초과, 잠시 대기 후 재시도...")
                time.sleep(5)
                return get_moving_averages(symbol)
                
            if response.status_code == 200:
                data = response.json()
                if data and len(data) > 0:
                    # 데이터 유효성 검사 추가
                    ma_value = safe_float(data[0].get('value'))
                    if ma_value > 0:  # 유효한 값만 저장
                        ma_key = f'MA{period}'
                        ma_data[ma_key] = ma_value

                        if period == 200 and len(data) >= 21:  # 충분한 데이터가 있는지 확인
                            month_ago_value = safe_float(data[20].get('value'))
                            if month_ago_value > 0:
                                ma_data['MA200_trend'] = ma_value > month_ago_value
            else:
                print(f"⚠️ {symbol} {period}일 이평선 데이터 가져오기 실패: {response.status_code}")

    except Exception as e:
        print(f"❌ {symbol} 이평선 데이터 가져오기 실패: {str(e)}")

    # 모든 필요한 데이터가 있는지 확인
    if not all(ma_data.values()):
        print(f"⚠️ {symbol} 일부 이평선 데이터 누락")
        return None

    return ma_data

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
        
        # 가격이 0이거나 거래량이 없는 경우 제외
        if price <= 0 or volume <= 0 or yearHigh <= 0:
            continue

        price_to_high_ratio = (price / yearHigh) * 100
        
        if not (price >= 10 and volume >= 1000000 and marketCap >= 500000000 and price_to_high_ratio >= 95):
            continue
            
        ma_data = get_moving_averages(stock['symbol'])
        if ma_data is None:  # 이평선 데이터 누락된 경우 스킵
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

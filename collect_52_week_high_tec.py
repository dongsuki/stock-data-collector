import os
import requests
from datetime import datetime, timedelta
from airtable import Airtable
import time
import numpy as np
import pandas as pd

# API 설정
FMP_API_KEY = "EApxNJTRwcXOrhy2IUqSeKV0gyH8gans"
AIRTABLE_API_KEY = "patBy8FRWWiG6P99a.a0670e9dd25c84d028c9f708af81d5f1fb164c3adeb1cee067d100075db8b748"
AIRTABLE_BASE_ID = "appAh82iPV3cH6Xx5"
TABLE_NAME = "미국주식 데이터"

MAX_WORKERS = 10  # 병렬 처리를 위한 최대 워커 수
MAX_RETRIES = 3   # API 호출 재시도 횟수

class APIRateLimiter:
    def __init__(self, calls_per_minute=300):
        self.calls_per_minute = calls_per_minute
        self.min_interval = 60.0 / calls_per_minute
        self.last_call_time = 0
        self.calls_made = 0
        self.reset_time = time.time() + 60

    def wait_if_needed(self):
        current_time = time.time()
        if current_time > self.reset_time:
            self.calls_made = 0
            self.reset_time = current_time + 60

        if self.calls_made >= self.calls_per_minute:
            sleep_time = self.reset_time - current_time
            if sleep_time > 0:
                time.sleep(sleep_time)
                self.calls_made = 0
                self.reset_time = time.time() + 60

        elapsed = current_time - self.last_call_time
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
            
        self.last_call_time = time.time()
        self.calls_made += 1

rate_limiter = APIRateLimiter(300)

def safe_float(value, default=0.0):
    try:
        if value is None:
            return default
        return float(value)
    except (ValueError, TypeError):
        return default

def get_delisted_stocks():
    rate_limiter.wait_if_needed()
    for _ in range(MAX_RETRIES):
        try:
            url = f"https://financialmodelingprep.com/api/v3/delisted-companies?apikey={FMP_API_KEY}"
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                return {item['symbol'] for item in response.json()}
        except Exception as e:
            print(f"❌ 상장폐지 종목 데이터 가져오기 실패: {str(e)}")
            time.sleep(2)
    return set()

def get_tradable_stocks():
    rate_limiter.wait_if_needed()
    for _ in range(MAX_RETRIES):
        try:
            url = f"https://financialmodelingprep.com/api/v3/available-traded/list?apikey={FMP_API_KEY}"
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                return {item['symbol'] for item in response.json()}
        except Exception as e:
            print(f"❌ 거래 가능한 종목 데이터 가져오기 실패: {str(e)}")
            time.sleep(2)
    return set()

def get_quotes():
    print("📡 데이터 수집 시작...")
    all_stocks = []
    
    def fetch_exchange_data(exchange):
        rate_limiter.wait_if_needed()
        for _ in range(MAX_RETRIES):
            try:
                url = f"https://financialmodelingprep.com/api/v3/quotes/{exchange}?apikey={FMP_API_KEY}"
                response = requests.get(url, timeout=30)
                if response.status_code == 200:
                    data = response.json()
                    print(f"📌 {exchange} 종목 수집 완료: {len(data)}개")
                    return data
            except Exception as e:
                print(f"❌ {exchange} 데이터 수집 실패: {str(e)}")
                time.sleep(2)
        return []

    with ThreadPoolExecutor(max_workers=2) as executor:
        future_to_exchange = {
            executor.submit(fetch_exchange_data, "NASDAQ"): "NASDAQ",
            executor.submit(fetch_exchange_data, "NYSE"): "NYSE"
        }
        
        for future in as_completed(future_to_exchange):
            exchange = future_to_exchange[future]
            try:
                data = future.result()
                all_stocks.extend(data)
            except Exception as e:
                print(f"❌ {exchange} 데이터 처리 실패: {str(e)}")

    print(f"✅ 총 수집 종목 수: {len(all_stocks)}개")
    return all_stocks

def get_historical_data(symbol):
    rate_limiter.wait_if_needed()
    for _ in range(MAX_RETRIES):
        try:
            url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}?apikey={FMP_API_KEY}"
            response = requests.get(url, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if 'historical' in data and data['historical']:
                    historical_data = sorted(data['historical'], 
                                          key=lambda x: x['date'], 
                                          reverse=True)
                    if len(historical_data) >= 252:  # 1년치 데이터 필요
                        return historical_data
                    else:
                        print(f"⚠️ {symbol}: 충분한 히스토리 데이터 없음")
                        return None
            else:
                print(f"⚠️ {symbol}: API 응답 에러 ({response.status_code})")
                
        except Exception as e:
            print(f"⚠️ {symbol}: 요청 실패 - {str(e)}")
            time.sleep(2)
    
    return None

def is_valid_us_stock(stock, delisted_stocks, tradable_stocks):
    """실제 거래 가능한 미국 주식인지 확인"""
    symbol = stock.get('symbol', '')
    exchange = stock.get('exchange', '')
    type = stock.get('type', '').lower()
    name = stock.get('name', '') or ''
    name = name.lower()

    if exchange not in {'NYSE', 'NASDAQ'}:
        return False

    if symbol in delisted_stocks:
        return False

    if symbol not in tradable_stocks:
        return False

    etf_keywords = ['etf', 'trust', 'fund']
    if 'etf' in type or any(keyword in name for keyword in etf_keywords):
        return False

    invalid_keywords = [
        'warrant', 'warrants', 'adr', 'preferred', 'acquisition',
        'right', 'rights', 'merger', 'spac', 'unit', 'notes',
        'bond', 'series', 'class', 'holding', 'holdings', 'partners', 'management'
    ]
    return not any(keyword in name for keyword in invalid_keywords)

def calculate_rs(symbol, historical_data):
    """RS 계산"""
    try:
        if not historical_data:
            return None
            
        dates_needed = [0, 63, 126, 189, 252]  # 필요한 거래일 수
        
        if len(historical_data) < max(dates_needed):
            return None
            
        prices = {}
        for date_index in dates_needed:
            if date_index < len(historical_data):
                try:
                    prices[date_index] = float(historical_data[date_index]['close'])
                except (KeyError, ValueError, TypeError):
                    return None
                    
        if len(prices) != len(dates_needed):
            return None

        quarters = [
            (prices[0] / prices[63] - 1) * 100,    # 최근 3개월
            (prices[63] / prices[126] - 1) * 100,  # 2분기
            (prices[126] / prices[189] - 1) * 100, # 3분기
            (prices[189] / prices[252] - 1) * 100  # 4분기
        ]

        weighted_return = (
            quarters[0] * 0.4 +  # 최근 3개월: 40%
            quarters[1] * 0.2 +  # 2분기: 20%
            quarters[2] * 0.2 +  # 3분기: 20%
            quarters[3] * 0.2    # 4분기: 20%
        )

        return weighted_return
        
    except Exception as e:
        print(f"⚠️ {symbol} RS 계산 중 오류 발생: {str(e)}")
        return None

def process_stock_with_rs(stock, delisted_stocks, tradable_stocks):
    """개별 종목 처리 및 RS 계산"""
    if not is_valid_us_stock(stock, delisted_stocks, tradable_stocks):
        return None
        
    symbol = stock['symbol']
    historical_data = get_historical_data(symbol)
    if historical_data:
        rs_value = calculate_rs(symbol, historical_data)
        if rs_value is not None:
            return {
                'symbol': symbol,
                'data': stock,
                'rs_value': rs_value
            }
    return None

def calculate_rs_rating(returns_dict):
    """RS 등급 계산 (1-99 스케일)"""
    returns = list(returns_dict.values())
    symbols = list(returns_dict.keys())
    
    ranks = pd.Series(returns).rank(ascending=False)
    n = len(returns)
    
    rs_ratings = ((n - ranks) / (n - 1) * 98) + 1
    
    return {symbol: rating for symbol, rating in zip(symbols, rs_ratings)}

def check_technical_conditions(stock, ma_data):
    """기술적 조건 확인"""
    try:
        current_price = safe_float(stock.get('price'))
        ma50 = safe_float(ma_data.get('MA50'))
        ma150 = safe_float(ma_data.get('MA150'))
        ma200 = safe_float(ma_data.get('MA200'))
        ma200_trend = ma_data.get('MA200_trend')
        year_low = safe_float(stock.get('yearLow'))

        if any(x is None or x <= 0 for x in [current_price, ma50, ma150, ma200, year_low]):
            return False

        conditions = {
            'current_price > ma150': current_price > ma150,
            'current_price > ma200': current_price > ma200,
            'ma150 > ma200': ma150 > ma200,
            'ma200_trend': ma200_trend,
            'ma50 > ma150': ma50 > ma150,
            'ma50 > ma200': ma50 > ma200,
            'current_price > ma50': current_price > ma50,
            'current_price > year_low*1.3': current_price > (year_low * 1.3)
        }

        return all(conditions.values())

    except Exception as e:
        print(f"⚠️ {stock.get('symbol')} 기술적 조건 확인 중 오류 발생: {e}")
        return False

def process_stocks():
    """종목 처리 및 RS 계산"""
    print("\n🔎 종목 처리 시작...")
    
    # 1. 기본 필터링을 위한 데이터 가져오기
    delisted_stocks = get_delisted_stocks()
    tradable_stocks = get_tradable_stocks()
    
    # 2. 모든 종목 데이터 가져오기
    all_stocks = get_quotes()
    
    # 3. 유효한 종목 필터링 및 RS 계산 (병렬 처리)
    print("\n📊 RS 계산 시작...")
    rs_data = {}
    valid_stocks = {}
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_stock = {
            executor.submit(process_stock_with_rs, stock, delisted_stocks, tradable_stocks): stock
            for stock in all_stocks
        }
        
        for future in as_completed(future_to_stock):
            result = future.result()
            if result:
                symbol = result['symbol']
                valid_stocks[symbol] = result['data']
                rs_data[symbol] = result['rs_value']
    
    # 4. RS 등급 계산
    rs_ratings = calculate_rs_ranking(rs_data)
    
    # 5. 52주 신고가 조건 및 기술적 조건 확인
    filtered_stocks = []
    for symbol, stock in valid_stocks.items():
        if check_high_and_technical_conditions(stock):
            stock_data = prepare_stock_data(stock)
            if symbol in rs_ratings:
                stock_data['rs_rating'] = rs_ratings[symbol]
            filtered_stocks.append(stock_data)
    
    print(f"\n✅ 필터링된 종목 수: {len(filtered_stocks)}개")
    return filtered_stocks

def check_high_and_technical_conditions(stock):
    price = safe_float(stock.get('price'))
    volume = safe_float(stock.get('volume'))
    yearHigh = safe_float(stock.get('yearHigh'))
    marketCap = safe_float(stock.get('marketCap'))
    
    try:
        price_to_high_ratio = (price / yearHigh) * 100 if yearHigh and yearHigh > 0 else 0
    except ZeroDivisionError:
        return False
    
    if not (price >= 10 and volume >= 1000000 and marketCap >= 500000000 and price_to_high_ratio >= 75):
        return False
        
    ma_data = get_moving_averages(stock['symbol'])
    if ma_data is None:
        return False
    
    return check_technical_conditions(stock, ma_data)

def prepare_stock_data(stock):
    price = safe_float(stock.get('price'))
    yearHigh = safe_float(stock.get('yearHigh'))
    
    try:
        price_to_high_ratio = (price / yearHigh) * 100 if yearHigh and yearHigh > 0 else 0
        change_percent = ((price - safe_float(stock.get('previousClose'))) / 
                         safe_float(stock.get('previousClose'))) * 100 if safe_float(stock.get('previousClose')) > 0 else 0
    except ZeroDivisionError:
        price_to_high_ratio = 0
        change_percent = 0
        
    return {
        'symbol': stock['symbol'],
        'price': price,
        'volume': stock.get('volume'),
        'yearHigh': yearHigh,
        'marketCap': stock.get('marketCap'),
        'name': stock['name'],
        'exchange': stock['exchange'],
        'price_to_high_ratio': price_to_high_ratio,
        'change_percent': change_percent
    }

def update_airtable(stocks):
    """Airtable 업데이트"""
    print("\n📡 Airtable 업데이트 시작...")
    airtable = Airtable(AIRTABLE_BASE_ID, TABLE_NAME, AIRTABLE_API_KEY)
    current_date = datetime.now().strftime("%Y-%m-%d")

    # 새 레코드 추가
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
            '신고가 비율(%)': stock['price_to_high_ratio'],
            'RS': stock.get('rs_rating', 0)  # RS 등급 추가
        }
        try:
            airtable.insert(record)
        except Exception as e:
            print(f"⚠️ Airtable 업데이트 실패 ({stock['symbol']}): {str(e)}")
            continue

    print("✅ Airtable 업데이트 완료!")

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

def get_historical_data(symbol):
    rate_limiter.wait_if_needed()
    max_retries = 3
    retry_delay = 2

    for attempt in range(max_retries):
        try:
            url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}?apikey={FMP_API_KEY}"
            response = requests.get(url, timeout=30)
            
            if response.status_code == 429:
                print(f"⚠️ {symbol}: API 호출 한도 초과, {retry_delay}초 후 재시도...")
                time.sleep(retry_delay)
                retry_delay *= 2
                continue
                
            if response.status_code == 200:
                data = response.json()
                if 'historical' in data and data['historical']:
                    historical_data = data['historical']
                    if len(historical_data) < 252:  # RS 계산을 위해 1년치 데이터 필요
                        return None
                    return sorted(historical_data, key=lambda x: x['date'], reverse=True)
            else:
                print(f"⚠️ {symbol}: API 응답 에러 (상태 코드: {response.status_code})")

        except Exception as e:
            print(f"⚠️ {symbol}: 요청 실패 - {str(e)}")
        
        time.sleep(retry_delay)
        retry_delay *= 2

    return None

def calculate_rs(symbol, historical_data):
    """RS 계산"""
    if not historical_data or len(historical_data) < 252:
        return None

    try:
        # 분기별 데이터 추출 (63일 간격)
        prices = [float(day['close']) for day in historical_data[:252]]
        if len(prices) < 252:
            return None

        # 분기별 수익률 계산
        quarters = [
            (prices[0] / prices[63] - 1) * 100,  # 최근 3개월
            (prices[63] / prices[126] - 1) * 100,  # 2분기
            (prices[126] / prices[189] - 1) * 100,  # 3분기
            (prices[189] / prices[252] - 1) * 100  # 4분기
        ]

        # 가중 평균 수익률 계산
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

def calculate_rs_ranking(returns_dict):
    """RS 등급 계산 (1-99 스케일)"""
    returns = list(returns_dict.values())
    symbols = list(returns_dict.keys())
    
    # 수익률로 순위 매기기
    ranks = pd.Series(returns).rank(ascending=False)
    n = len(returns)
    
    # RS 등급 계산: ((종목 수 - 현재 종목의 순위) / (종목 수 - 1) * 98) + 1
    rs_ratings = ((n - ranks) / (n - 1) * 98) + 1
    
    return {symbol: rating for symbol, rating in zip(symbols, rs_ratings)}

def is_valid_us_stock(stock, delisted_stocks, tradable_stocks):
    """실제 거래 가능한 미국 주식인지 확인"""
    symbol = stock.get('symbol', '')
    exchange = stock.get('exchange', '')
    type = stock.get('type', '').lower()
    name = stock.get('name', '') or ''
    name = name.lower()

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
    return not any(keyword in name for keyword in invalid_keywords)

def process_stocks():
    """종목 처리 및 RS 계산"""
    print("\n🔎 종목 처리 시작...")
    
    # 1. 기본 필터링을 위한 데이터 가져오기
    delisted_stocks = get_delisted_stocks()
    tradable_stocks = get_tradable_stocks()
    
    # 2. 모든 종목 데이터 가져오기
    all_stocks = get_quotes()
    
    # 3. 유효한 종목 필터링 및 RS 계산
    valid_stocks = {}
    rs_returns = {}
    
    for stock in all_stocks:
        if is_valid_us_stock(stock, delisted_stocks, tradable_stocks):
            valid_stocks[stock['symbol']] = stock
            
            # RS 계산을 위한 히스토리 데이터 가져오기
            historical_data = get_historical_data(stock['symbol'])
            if historical_data:
                rs_return = calculate_rs(stock['symbol'], historical_data)
                if rs_return is not None:
                    rs_returns[stock['symbol']] = rs_return
    
    # 4. RS 등급 계산
    rs_ratings = calculate_rs_ranking(rs_returns)
    
    # 5. 52주 신고가 조건 및 기술적 조건 확인
    filtered_stocks = []
    for symbol, stock in valid_stocks.items():
        if check_high_and_technical_conditions(stock):
            stock_data = prepare_stock_data(stock)
            if symbol in rs_ratings:
                stock_data['rs_rating'] = rs_ratings[symbol]
            filtered_stocks.append(stock_data)
    
    return filtered_stocks

def check_high_and_technical_conditions(stock):
    """52주 신고가 및 기술적 조건 확인"""
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
    """Airtable에 저장할 데이터 준비"""
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
        airtable.insert(record)
    print("✅ Airtable 업데이트 완료!")

def main():
    filtered_stocks = process_stocks()
    if filtered_stocks:
        update_airtable(filtered_stocks)

if __name__ == "__main__":
    main()

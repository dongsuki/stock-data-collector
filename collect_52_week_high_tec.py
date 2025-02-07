import os
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from airtable import Airtable

# 환경 변수 설정
FMP_API_KEY = os.getenv('FMP_API_KEY')
AIRTABLE_API_KEY = os.getenv('AIRTABLE_API_KEY')
AIRTABLE_BASE_ID = os.getenv('AIRTABLE_BASE_ID')
TABLE_NAME = "미국주식 데이터"

MAX_RETRIES = 3
CHUNK_SIZE = 30

class APIRateLimiter:
    def __init__(self, calls_per_minute=250):
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
            sleep_time = self.reset_time - current_time + 1
            if sleep_time > 0:
                print(f"Rate limit reached, waiting {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)
                self.calls_made = 0
                self.reset_time = time.time() + 60

        elapsed = current_time - self.last_call_time
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed + 0.1)

        self.last_call_time = time.time()
        self.calls_made += 1

rate_limiter = APIRateLimiter(250)

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

def get_historical_data_batch(symbols):
    rate_limiter.wait_if_needed()
    symbols_str = ','.join(symbols[:CHUNK_SIZE])
    for _ in range(MAX_RETRIES):
        try:
            url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbols_str}?apikey={FMP_API_KEY}"
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                data = response.json()
                return data.get('historicalStockList', [])
            elif response.status_code == 429:
                print("⚠️ Rate limit reached, waiting...")
                time.sleep(10)
                continue
        except Exception as e:
            print(f"⚠️ 배치 데이터 요청 실패: {str(e)}")
            time.sleep(2)
    return []

def is_valid_us_stock(stock, delisted_stocks, tradable_stocks):
    # 기존 조건: symbol.isalnum()으로 알파벳과 숫자만 허용
    if not all([stock.get('symbol'), stock.get('exchange'), stock.get('name')]):
        return False

    symbol = stock['symbol']
    if not symbol.isalnum():
        return False

    if stock['exchange'] not in {'NYSE', 'NASDAQ'}:
        return False

    if symbol in delisted_stocks or symbol not in tradable_stocks:
        return False

    invalid_keywords = {
        'etf', 'trust', 'fund', 'warrant', 'warrants', 'adr', 
        'preferred', 'acquisition', 'right', 'rights', 'merger', 
        'spac', 'unit', 'notes', 'bond', 'series', 'class', 
        'holding', 'holdings', 'partners', 'management'
    }
    name_lower = stock['name'].lower()
    return not any(keyword in name_lower for keyword in invalid_keywords)

def calculate_rs(symbol, historical_data):
    """
    RS 계산: 현재가 대비 252 거래일 전 가격을 이용.
    만약 반환된 히스토리 데이터 길이가 252일 미만이면,
    API 설정이나 파라미터 문제일 수 있으므로 로그로 안내하고 None 반환.
    """
    try:
        if not historical_data or len(historical_data) < 252:
            print(f"⚠️ {symbol}: 충분한 히스토리 데이터 없음 (현재 {len(historical_data) if historical_data else 0}일). API 설정 또는 파라미터를 확인하세요.")
            return None

        # 고정 인덱스: 0, 63, 126, 189, 252
        indices = [0, 63, 126, 189, 252]
        prices = {}
        for idx in indices:
            try:
                prices[idx] = float(historical_data[idx]['close'])
            except Exception as e:
                print(f"⚠️ {symbol}: 인덱스 {idx}에서 가격 데이터를 가져올 수 없습니다: {e}")
                return None

        quarters = [
            (prices[0] / prices[63] - 1) * 100,
            (prices[63] / prices[126] - 1) * 100,
            (prices[126] / prices[189] - 1) * 100,
            (prices[189] / prices[252] - 1) * 100
        ]
        weighted_return = quarters[0] * 0.4 + quarters[1] * 0.2 + quarters[2] * 0.2 + quarters[3] * 0.2
        return weighted_return

    except Exception as e:
        print(f"⚠️ {symbol}: RS 계산 중 오류 발생: {e}")
        return None

def calculate_rs_rating(returns_dict):
    import pandas as pd
    returns = list(returns_dict.values())
    symbols = list(returns_dict.keys())
    ranks = pd.Series(returns).rank(ascending=False)
    n = len(returns)
    rs_ratings = ((n - ranks) / (n - 1) * 98) + 1
    return {symbol: rating for symbol, rating in zip(symbols, rs_ratings)}

def calculate_moving_averages(historical_data):
    if not historical_data or len(historical_data) < 200:
        return None

    closes = []
    for day in historical_data:
        try:
            closes.append(float(day['close']))
        except (ValueError, TypeError):
            continue

    if len(closes) < 200:
        return None

    ma50 = sum(closes[:50]) / 50
    ma150 = sum(closes[:150]) / 150
    ma200 = sum(closes[:200]) / 200
    ma200_prev = sum(closes[20:220]) / 200
    ma200_trend = ma200 > ma200_prev

    return {'MA50': ma50, 'MA150': ma150, 'MA200': ma200, 'MA200_trend': ma200_trend}

def check_technical_conditions(stock, historical_data):
    symbol = stock.get('symbol')
    ma_data = calculate_moving_averages(historical_data)
    if not ma_data:
        print(f"⚠️ {symbol}: 이동평균 계산 실패")
        return False

    try:
        current_price = safe_float(stock.get('price'))
        ma50 = safe_float(ma_data.get('MA50'))
        ma150 = safe_float(ma_data.get('MA150'))
        ma200 = safe_float(ma_data.get('MA200'))
        ma200_trend = ma_data.get('MA200_trend')
        year_low = safe_float(stock.get('yearLow'))

        if any(x is None or x <= 0 for x in [current_price, ma50, ma150, ma200, year_low]):
            print(f"⚠️ {symbol}: 유효하지 않은 데이터 - price={current_price}, MA50={ma50}, MA150={ma150}, MA200={ma200}, yearLow={year_low}")
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

        failed_conditions = [name for name, result in conditions.items() if not result]
        if failed_conditions:
            print(f"❌ {symbol} 불만족 조건들: {', '.join(failed_conditions)}")
            print(f"   현재가: {current_price}, MA50: {ma50}, MA150: {ma150}, MA200: {ma200}")
            print(f"   52주 저가: {year_low}, 저가의 130%: {year_low * 1.3}")
            return False

        print(f"✅ {symbol} 모든 기술적 조건 만족")
        return True

    except Exception as e:
        print(f"⚠️ {symbol} 기술적 조건 확인 중 오류 발생: {e}")
        return False

def check_high_conditions(stock):
    symbol = stock.get('symbol')
    price = safe_float(stock.get('price'))
    volume = safe_float(stock.get('volume'))
    yearHigh = safe_float(stock.get('yearHigh'))
    marketCap = safe_float(stock.get('marketCap'))

    try:
        price_to_high_ratio = (price / yearHigh) * 100 if yearHigh and yearHigh > 0 else 0
        conditions = {
            'price >= 10': price >= 10,
            'volume >= 1000000': volume >= 1000000,
            'marketCap >= 500000000': marketCap >= 500000000,
            'price_to_high_ratio >= 75': price_to_high_ratio >= 75
        }

        failed_conditions = [name for name, result in conditions.items() if not result]
        if failed_conditions:
            print(f"❌ {symbol} 52주 신고가 조건 불만족: {', '.join(failed_conditions)}")
            print(f"   가격: {price}, 거래량: {volume}, 시가총액: {marketCap}")
            print(f"   신고가 비율: {price_to_high_ratio:.1f}%")
            return False

        print(f"✅ {symbol} 52주 신고가 조건 만족")
        return True

    except ZeroDivisionError:
        print(f"⚠️ {symbol} 52주 고가가 0입니다")
        return False

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

def process_stocks():
    print("\n🔎 종목 처리 시작...")
    delisted_stocks = get_delisted_stocks()
    tradable_stocks = get_tradable_stocks()
    print(f"상장폐지 종목 수: {len(delisted_stocks)}")
    print(f"거래가능 종목 수: {len(tradable_stocks)}")

    all_stocks = get_quotes()
    valid_stocks = {}
    for stock in all_stocks:
        symbol = stock.get('symbol')
        if not is_valid_us_stock(stock, delisted_stocks, tradable_stocks):
            print(f"✗ {symbol} 제외 - 기본 필터링")
            continue
        valid_stocks[symbol] = stock

    print(f"\n기본 필터링 후 남은 종목 수: {len(valid_stocks)}")
    print(f"\n📊 {len(valid_stocks)} 종목에 대한 RS 계산 시작...")
    rs_data = {}
    historical_data_map = {}
    symbols = list(valid_stocks.keys())
    for i in range(0, len(symbols), CHUNK_SIZE):
        chunk = symbols[i:i+CHUNK_SIZE]
        print(f"Processing batch {i//CHUNK_SIZE + 1}/{(len(symbols) + CHUNK_SIZE - 1)//CHUNK_SIZE}")
        batch_data = get_historical_data_batch(chunk)
        for symbol_data in batch_data:
            symbol = symbol_data.get('symbol')
            if symbol:
                historical = symbol_data.get('historical', [])
                if len(historical) >= 252:
                    historical_data_map[symbol] = historical
                    rs_value = calculate_rs(symbol, historical)
                    if rs_value is not None:
                        rs_data[symbol] = rs_value
                    print(f"✅ {symbol} RS값: {rs_value}")
                else:
                    print(f"⚠️ {symbol}: 충분한 히스토리 데이터 없음 ({len(historical)}일)")

    print(f"\nRS 계산 완료된 종목 수: {len(rs_data)}")
    rs_ratings = calculate_rs_rating(rs_data)
    filtered_stocks = []
    technical_check_count = 0
    high_conditions_count = 0

    for symbol, stock in valid_stocks.items():
        if symbol in historical_data_map:
            if check_high_conditions(stock):
                high_conditions_count += 1
                if check_technical_conditions(stock, historical_data_map[symbol]):
                    technical_check_count += 1
                    stock_data = prepare_stock_data(stock)
                    if symbol in rs_ratings:
                        stock_data['rs_rating'] = rs_ratings[symbol]
                        print(f"✅ {symbol} 최종 RS등급: {rs_ratings[symbol]:.2f}")
                    filtered_stocks.append(stock_data)

    print(f"\n52주 신고가 조건 만족 종목 수: {high_conditions_count}")
    print(f"기술적 조건까지 만족 종목 수: {technical_check_count}")
    print(f"\n✅ 최종 필터링된 종목 수: {len(filtered_stocks)}개")
    return filtered_stocks

def update_airtable(stocks):
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
            'RS': stock.get('rs_rating', 0)
        }
        try:
            airtable.insert(record)
        except Exception as e:
            print(f"⚠️ Airtable 업데이트 실패 ({stock['symbol']}): {str(e)}")
            continue
    print("✅ Airtable 업데이트 완료!")

def main():
    try:
        print("\n🚀 프로그램 시작...")
        print(f"FMP_API_KEY: {'설정됨' if FMP_API_KEY else '미설정'}")
        print(f"AIRTABLE_API_KEY: {'설정됨' if AIRTABLE_API_KEY else '미설정'}")
        print(f"AIRTABLE_BASE_ID: {'설정됨' if AIRTABLE_BASE_ID else '미설정'}")
        if not all([FMP_API_KEY, AIRTABLE_API_KEY, AIRTABLE_BASE_ID]):
            raise ValueError("필수 환경 변수가 설정되지 않았습니다.")

        filtered_stocks = process_stocks()
        if filtered_stocks:
            print(f"\n📊 조건을 만족하는 종목 수: {len(filtered_stocks)}개")
            update_airtable(filtered_stocks)
        else:
            print("\n⚠️ 조건을 만족하는 종목이 없습니다.")
    except Exception as e:
        print(f"\n❌ 프로그램 실행 중 오류 발생: {str(e)}")
        raise
    finally:
        print("\n✨ 프로그램 종료")

if __name__ == "__main__":
    main()

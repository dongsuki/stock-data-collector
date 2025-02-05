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
    def __init__(self, calls_per_minute=300):
        self.calls_per_minute = calls_per_minute
        self.min_interval = 60.0 / calls_per_minute
        self.last_call_time = 0
        self.calls_made = 0
        self.reset_time = time.time() + 60

    def wait_if_needed(self):
        current_time = time.time()
        
        # 1분이 지났으면 카운터 리셋
        if current_time > self.reset_time:
            self.calls_made = 0
            self.reset_time = current_time + 60

        # 호출 횟수가 제한에 도달하면 남은 시간만큼 대기
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
    max_retries = 3
    retry_delay = 2

    for attempt in range(max_retries):
        try:
            url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}?apikey={FMP_API_KEY}"
            response = requests.get(url, timeout=30)
            
            # API 호출 한도 초과 시 재시도
            if response.status_code == 429:
                print(f"⚠️ {symbol}: API 호출 한도 초과, {retry_delay}초 후 재시도...")
                time.sleep(retry_delay)
                retry_delay *= 2  # 다음 재시도까지 대기 시간 증가
                continue
                
            if response.status_code == 200:
                data = response.json()
                if 'historical' in data and data['historical']:
                    historical_data = data['historical']
                    
                    # 데이터가 충분한지 확인
                    if len(historical_data) < 200:
                        print(f"⚠️ {symbol}: 충분한 히스토리 데이터 없음 (필요: 200일, 실제: {len(historical_data)}일)")
                        return None
                    
                    # 종가 데이터가 있는지 확인
                    valid_data = [x for x in historical_data if 'close' in x and x['close'] is not None]
                    if len(valid_data) < 200:
                        print(f"⚠️ {symbol}: 유효한 종가 데이터 부족 (필요: 200일, 실제: {len(valid_data)}일)")
                        return None
                        
                    # 날짜순 정렬 (최신 데이터가 앞에 오도록)
                    return sorted(valid_data, key=lambda x: x['date'], reverse=True)
                else:
                    print(f"⚠️ {symbol}: 히스토리 데이터 없음")
                    return None
            else:
                print(f"⚠️ {symbol}: API 응답 에러 (상태 코드: {response.status_code})")

        except requests.exceptions.Timeout:
            print(f"⚠️ {symbol}: 요청 시간 초과")
        except requests.exceptions.RequestException as e:
            print(f"⚠️ {symbol}: 요청 실패 - {str(e)}")
        except Exception as e:
            print(f"❌ {symbol}: 예상치 못한 에러 - {str(e)}")
        
        time.sleep(retry_delay)
        retry_delay *= 2  # 다음 재시도까지 대기 시간 증가

    return None
    
def calculate_moving_averages(historical_data):
    """이동평균선 계산"""
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

    # 이동평균선 계산
    ma50 = sum(closes[:50]) / 50
    ma150 = sum(closes[:150]) / 150
    ma200 = sum(closes[:200]) / 200

    # 200일 이평선 추세 확인
    ma200_prev = sum(closes[20:220]) / 200
    ma200_trend = ma200 > ma200_prev

    return {
        'MA50': ma50,
        'MA150': ma150,
        'MA200': ma200,
        'MA200_trend': ma200_trend
    }

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
    try:
        symbol = stock.get('symbol')
        current_price = safe_float(stock.get('price'))
        ma50 = safe_float(ma_data.get('MA50'))
        ma150 = safe_float(ma_data.get('MA150'))
        ma200 = safe_float(ma_data.get('MA200'))
        ma200_trend = ma_data.get('MA200_trend')
        year_low = safe_float(stock.get('yearLow'))

        if any(x is None or x <= 0 for x in [current_price, ma50, ma150, ma200, year_low]):
            print(f"⚠️ {symbol} 일부 데이터 누락 또는 0 이하: price={current_price}, MA50={ma50}, MA150={ma150}, MA200={ma200}, yearLow={year_low}")
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

        # 각 조건의 결과를 출력
        if not all(conditions.values()):
            failed_conditions = [name for name, result in conditions.items() if not result]
            print(f"❌ {symbol} 불만족 조건들: {', '.join(failed_conditions)}")
            print(f"   현재가: {current_price}, MA50: {ma50}, MA150: {ma150}, MA200: {ma200}")
            print(f"   52주 저가: {year_low}, 저가의 130%: {year_low * 1.3}")
            return False
        
        print(f"✅ {symbol} 모든 기술적 조건 만족")
        return True

    except Exception as e:
        print(f"⚠️ {stock.get('symbol')} 기술적 조건 확인 중 오류 발생: {e}")
        return False

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
        
        try:
            price_to_high_ratio = (price / yearHigh) * 100 if yearHigh and yearHigh > 0 else 0
        except ZeroDivisionError:
            continue
        
        if not (price >= 10 and volume >= 1000000 and marketCap >= 500000000 and price_to_high_ratio >= 95):
            continue
            
        ma_data = get_moving_averages(stock['symbol'])
        if ma_data is None:
            continue
        
        if check_technical_conditions(stock, ma_data):
            try:
                change_percent = ((price - safe_float(stock.get('previousClose'))) / 
                                safe_float(stock.get('previousClose'))) * 100 if safe_float(stock.get('previousClose')) > 0 else 0
            except ZeroDivisionError:
                change_percent = 0
            
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
            print(f"✅ 조건 만족 종목 발견: {stock['symbol']} (신고가대비: {price_to_high_ratio:.1f}%)")

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

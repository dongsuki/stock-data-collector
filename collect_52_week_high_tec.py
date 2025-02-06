import os
import requests
import time
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from typing import Set, Dict, List
from airtable import Airtable

# API 설정
FMP_API_KEY = "EApxNJTRwcXOrhy2IUqSeKV0gyH8gans"
AIRTABLE_API_KEY = "patBy8FRWWiG6P99a.a0670e9dd25c84d028c9f708af81d5f1fb164c3adeb1cee067d100075db8b748"
AIRTABLE_BASE_ID = "appAh82iPV3cH6Xx5"
TABLE_NAME = "미국주식 데이터"

# =======================================================
# 1. RateLimiter 설정
# =======================================================
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
        return float(value)
    except (ValueError, TypeError):
        return default

# =======================================================
# 2. 기본 자료 수집 (상장폐지, 거래가능, 시세 데이터)
# =======================================================
def get_delisted_stocks() -> Set[str]:
    url = f"https://financialmodelingprep.com/api/v3/delisted-companies?apikey={FMP_API_KEY}"
    for _ in range(3):
        rate_limiter.wait_if_needed()
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 200:
                data = r.json()
                return {item['symbol'] for item in data}
            else:
                print(f"⚠️ delisted-companies API 응답 에러: {r.status_code}")
        except Exception as e:
            print(f"❌ 상장폐지 종목 데이터 가져오기 실패: {str(e)}")
            time.sleep(1)
    return set()

def get_tradable_stocks() -> Set[str]:
    url = f"https://financialmodelingprep.com/api/v3/available-traded/list?apikey={FMP_API_KEY}"
    for _ in range(3):
        rate_limiter.wait_if_needed()
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 200:
                data = r.json()
                return {item['symbol'] for item in data}
            else:
                print(f"⚠️ available-traded/list API 응답 에러: {r.status_code}")
        except Exception as e:
            print(f"❌ 거래 가능한 종목 데이터 가져오기 실패: {str(e)}")
            time.sleep(1)
    return set()

def fetch_exchange_data(exchange: str) -> List[Dict]:
    url = f"https://financialmodelingprep.com/api/v3/quotes/{exchange}?apikey={FMP_API_KEY}"
    for _ in range(3):
        rate_limiter.wait_if_needed()
        try:
            resp = requests.get(url, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                print(f"📌 {exchange} 종목 수집 완료: {len(data)}개")
                return data
            else:
                print(f"⚠️ {exchange} 데이터 응답 에러: {resp.status_code}")
        except Exception as e:
            print(f"❌ {exchange} 데이터 수집 실패: {str(e)}")
            time.sleep(1)
    return []

def get_quotes():
    print("📡 데이터 수집 시작...")
    nyse_stocks = fetch_exchange_data("NYSE")
    nasdaq_stocks = fetch_exchange_data("NASDAQ")
    all_stocks = nyse_stocks + nasdaq_stocks
    print(f"✅ 총 수집 종목 수: {len(all_stocks)}개")
    return all_stocks

# =======================================================
# 3. 로컬 필터링 (ETF 및 기타 제외 조건 적용)
# =======================================================
def is_valid_us_stock(stock: Dict, delisted: Set[str], tradable: Set[str]) -> bool:
    symbol = stock.get('symbol', '')
    exchange = stock.get('exchange', '')
    name = (stock.get('name') or '').lower()
    if symbol in delisted:
        return False
    if symbol not in tradable:
        return False
    if exchange not in {'NYSE', 'NASDAQ'}:
        return False
    # ETF, Trust, Fund 관련 키워드 제외
    etf_keywords = ['etf', 'trust', 'fund']
    if any(k in name for k in etf_keywords):
        return False
    invalid_keywords = [
        'warrant', 'warrants', 'adr', 'preferred', 'acquisition',
        'right', 'rights', 'merger', 'spac', 'unit', 'notes',
        'bond', 'series', 'class', 'holding', 'holdings', 'partners',
        'management'
    ]
    if any(k in name for k in invalid_keywords):
        return False
    return True

# =======================================================
# 4. 배치로 히스토리 데이터 조회 (최소 220일 필요: 1년치 데이터 + 추세 계산)
# =======================================================
MAX_BATCH_RETRIES = 5
def get_historical_data_batch(symbols: List[str], chunk_size=10) -> Dict[str, List[Dict]]:
    """
    여러 심볼을 묶어서 한 번에 1년치(252 거래일) 가격 데이터를 가져옴.
    최소 220일 이상의 데이터가 필요 (추세 계산 위해).
    반환: { symbol: [ {date, open, high, low, close, ...}, ... ], ... }
    """
    result = {}
    for i in range(0, len(symbols), chunk_size):
        chunk_symbols = symbols[i:i+chunk_size]
        joined_symbols = ",".join(chunk_symbols)
        url = (f"https://financialmodelingprep.com/api/v3/historical-price-full/"
               f"{joined_symbols}?timeseries=252&apikey={FMP_API_KEY}")
        success = False
        for attempt in range(MAX_BATCH_RETRIES):
            rate_limiter.wait_if_needed()
            try:
                resp = requests.get(url, timeout=30)
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, dict) and "historicalStockList" in data:
                        for entry in data["historicalStockList"]:
                            sym = entry.get("symbol")
                            hist = entry.get("historical", [])
                            if hist:
                                result[sym] = sorted(hist, key=lambda x: x['date'], reverse=True)
                    elif isinstance(data, dict) and "symbol" in data and "historical" in data:
                        sym = data["symbol"]
                        hist = data["historical"]
                        if hist:
                            result[sym] = sorted(hist, key=lambda x: x['date'], reverse=True)
                    elif isinstance(data, list):
                        for item in data:
                            sym = item.get("symbol")
                            hist = item.get("historical", [])
                            if hist:
                                result[sym] = sorted(hist, key=lambda x: x['date'], reverse=True)
                    success = True
                    break
                elif resp.status_code == 429:
                    print(f"⚠️ 배치 요청 429 발생: {joined_symbols} - 재시도 {attempt+1}/{MAX_BATCH_RETRIES}")
                    time.sleep(10)
                else:
                    print(f"⚠️ 배치 히스토리 요청 에러({resp.status_code}): {joined_symbols}")
                    break
            except Exception as e:
                print(f"⚠️ 배치 히스토리 요청 실패: {joined_symbols} -> {e}")
                time.sleep(5)
        if not success:
            print(f"❌ {joined_symbols} 에 대한 데이터 조회 실패")
    return result

# =======================================================
# 5. RS(가중 1년 수익률) 계산 및 RS 등급 산출
# =======================================================
def calculate_weighted_return(historical: List[Dict]) -> float:
    if len(historical) < 252:
        return None
    # 내림차순 정렬된 데이터를 기준으로 필요한 인덱스: 0, 63, 126, 189, 251
    needed_idx = [0, 63, 126, 189, 251]
    try:
        prices = {i: float(historical[i]['close']) for i in needed_idx}
        q1 = (prices[0] / prices[63] - 1) * 100
        q2 = (prices[63] / prices[126] - 1) * 100
        q3 = (prices[126] / prices[189] - 1) * 100
        q4 = (prices[189] / prices[251] - 1) * 100
        weighted_return = q1 * 0.4 + q2 * 0.2 + q3 * 0.2 + q4 * 0.2
        return weighted_return
    except Exception:
        return None

def calculate_rs_rating(returns_dict: Dict[str, float]) -> Dict[str, float]:
    symbols = list(returns_dict.keys())
    rets = list(returns_dict.values())
    s = pd.Series(rets)
    ranks = s.rank(ascending=False)
    n = len(rets)
    rs_values = ((n - ranks) / (n - 1) * 98) + 1
    return {sym: rs for sym, rs in zip(symbols, rs_values)}

# =======================================================
# 6. 52주 신고가 및 기술적 조건 체크 (사용자가 제시한 조건 그대로 적용)
# =======================================================
def check_high_and_technical_conditions(
    symbol: str, 
    historical: List[Dict], 
    rs_value: float,
    rs_threshold=70,
    high_ratio=0.75
) -> bool:
    # RS 기준
    if rs_value < rs_threshold:
        return False
    # 최소 220일 데이터 필요 (추세 계산 포함)
    if not historical or len(historical) < 220:
        return False
    closes = [safe_float(x.get('close')) for x in historical]
    current_price = closes[0]
    year_high = max(closes)
    if year_high == 0 or (current_price / year_high) < high_ratio:
        return False
    # 이동평균 계산 (최근 50, 150, 200일)
    ma50 = np.mean(closes[:50])
    ma150 = np.mean(closes[:150])
    ma200 = np.mean(closes[:200])
    # 200일 이평선 추세 확인: 20일 전부터 220일 전까지의 200일 평균과 비교
    ma200_prev = sum(closes[20:220]) / 200
    ma200_trend = ma200 > ma200_prev

    conditions = {
        'current_price > ma150': current_price > ma150,
        'current_price > ma200': current_price > ma200,
        'ma150 > ma200': ma150 > ma200,
        'ma200_trend': ma200_trend,
        'ma50 > ma150': ma50 > ma150,
        'ma50 > ma200': ma50 > ma200,
        'current_price > ma50': current_price > ma50,
    }
    return all(conditions.values())

# =======================================================
# 7. Airtable 업데이트 (배치 처리)
# =======================================================
def update_airtable(stocks: List[Dict]):
    print("\n📡 Airtable 업데이트 시작...")
    airtable = Airtable(AIRTABLE_BASE_ID, TABLE_NAME, AIRTABLE_API_KEY)
    current_date = datetime.now().strftime("%Y-%m-%d")
    batch_size = 10
    for i in range(0, len(stocks), batch_size):
        batch = stocks[i:i+batch_size]
        records = []
        for s in batch:
            record = {
                'fields': {
                    '티커': s['symbol'],
                    '종목명': s['name'],
                    '현재가': s['price'],
                    '등락률': s['change_percent'],
                    '거래량': s['volume'],
                    '시가총액': s['marketCap'],
                    '업데이트 시간': current_date,
                    '분류': "52주_신고가_근접",
                    '거래소 정보': s['exchange'],
                    '신고가 비율(%)': s['price_to_high_ratio'],
                    'RS': s.get('rs_rating', 0)
                }
            }
            records.append(record)
        try:
            airtable.batch_insert(records)
        except Exception as e:
            print(f"⚠️ Airtable 업데이트 실패: {str(e)}")
    print("✅ Airtable 업데이트 완료!")

# =======================================================
# 8. 메인 프로세스
# =======================================================
def main():
    try:
        print("\n🚀 프로그램 시작...")
        print(f"FMP_API_KEY: {'설정됨' if FMP_API_KEY else '미설정'}")
        print(f"AIRTABLE_API_KEY: {'설정됨' if AIRTABLE_API_KEY else '미설정'}")
        print(f"AIRTABLE_BASE_ID: {'설정됨' if AIRTABLE_BASE_ID else '미설정'}")
        if not all([FMP_API_KEY, AIRTABLE_API_KEY, AIRTABLE_BASE_ID]):
            raise ValueError("필수 환경 변수가 설정되지 않았습니다.")

        print("🔎 종목 처리 시작...")
        delisted_stocks = get_delisted_stocks()
        tradable_stocks = get_tradable_stocks()
        all_stocks = get_quotes()

        valid_symbols = []
        symbol_to_data = {}
        for st in all_stocks:
            if is_valid_us_stock(st, delisted_stocks, tradable_stocks):
                sym = st['symbol']
                valid_symbols.append(sym)
                symbol_to_data[sym] = st

        print(f"✔️ 사전 필터 후 종목 수: {len(valid_symbols)}개")

        # 배치로 1년치(최소 252일) 시세 데이터 조회 (청크 크기: 10)
        hist_map = get_historical_data_batch(valid_symbols, chunk_size=10)
        print(f"✔️ 1년치 시세 확보 종목 수: {len(hist_map)}개")

        # 각 종목의 가중 1년 수익률 계산
        returns_dict = {}
        for sym in hist_map:
            wret = calculate_weighted_return(hist_map[sym])
            if wret is not None:
                returns_dict[sym] = wret

        rs_dict = calculate_rs_rating(returns_dict)

        # 52주 신고가 및 기술적 조건 체크
        filtered_stocks = []
        for sym, rs_val in rs_dict.items():
            hist = hist_map.get(sym, [])
            if check_high_and_technical_conditions(sym, hist, rs_val):
                stock_info = symbol_to_data[sym]
                current_price = safe_float(stock_info.get('price'))
                previous_close = safe_float(stock_info.get('previousClose'))
                year_high = max(safe_float(x['close']) for x in hist)
                price_to_high_ratio = (current_price / year_high) * 100 if year_high > 0 else 0
                change_percent = ((current_price - previous_close) / previous_close) * 100 if previous_close else 0
                prepared = {
                    'symbol': sym,
                    'name': stock_info.get('name', ''),
                    'price': current_price,
                    'volume': safe_float(stock_info.get('volume')),
                    'marketCap': safe_float(stock_info.get('marketCap')),
                    'exchange': stock_info.get('exchange'),
                    'price_to_high_ratio': price_to_high_ratio,
                    'change_percent': change_percent,
                    'rs_rating': rs_val
                }
                filtered_stocks.append(prepared)

        print(f"\n✅ 필터링된 종목 수: {len(filtered_stocks)}개")
        if filtered_stocks:
            update_airtable(filtered_stocks)
        else:
            print("⚠️ 조건을 만족하는 종목이 없습니다.")

    except Exception as e:
        print(f"\n❌ 프로그램 실행 중 오류 발생: {str(e)}")
        raise
    finally:
        print("\n✨ 프로그램 종료")

if __name__ == "__main__":
    main()

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

# =========================================
# 1) RateLimiter 설정
# =========================================
class APIRateLimiter:
    def __init__(self, calls_per_minute=300):
        self.calls_per_minute = calls_per_minute
        self.min_interval = 60.0 / calls_per_minute
        self.last_call_time = 0
        self.calls_made = 0
        self.reset_time = time.time() + 60

    def wait_if_needed(self):
        current_time = time.time()
        # 1분이 지나면 카운터 리셋
        if current_time > self.reset_time:
            self.calls_made = 0
            self.reset_time = current_time + 60

        # 이미 calls_per_minute 소진 시에는 다음 리셋까지 대기
        if self.calls_made >= self.calls_per_minute:
            sleep_time = self.reset_time - current_time
            if sleep_time > 0:
                time.sleep(sleep_time)
                self.calls_made = 0
                self.reset_time = time.time() + 60

        # 마지막 호출 후 min_interval보다 빠르면 대기
        elapsed = current_time - self.last_call_time
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)

        # 호출 처리
        self.last_call_time = time.time()
        self.calls_made += 1

rate_limiter = APIRateLimiter(300)

def safe_float(value, default=0.0):
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

# =========================================
# 2) 기본 자료 수집(상장폐지, tradable 목록, quotes)
# =========================================
def get_delisted_stocks() -> Set[str]:
    """상장폐지된 종목 목록"""
    url = f"https://financialmodelingprep.com/api/v3/delisted-companies?apikey={FMP_API_KEY}"
    for _ in range(3):  # 최대 3회 재시도
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
    """거래 가능한 종목 목록"""
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
    """NYSE, NASDAQ 종목 정보 가져오기"""
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

# =========================================
# 3) 로컬 필터링
# =========================================
def is_valid_us_stock(stock: Dict, delisted: Set[str], tradable: Set[str]) -> bool:
    """실제로 거래 가능한 미국 주식인지 확인"""
    symbol = stock.get('symbol', '')
    exchange = stock.get('exchange', '')
    name = (stock.get('name') or '').lower()
    # 기본 필터
    if symbol in delisted:
        return False
    if symbol not in tradable:
        return False
    if exchange not in {'NYSE','NASDAQ'}:
        return False

    # ETF, 펀드, 신탁, 기타 제외
    etf_keywords = ['etf','trust','fund']
    if any(k in name for k in etf_keywords):
        return False

    # 불필요 키워드
    invalid_keywords = [
        'warrant', 'warrants', 'adr', 'preferred', 'acquisition',
        'right', 'rights', 'merger', 'spac', 'unit', 'notes',
        'bond', 'series', 'class', 'holding', 'holdings', 'partners',
        'management'
    ]
    if any(k in name for k in invalid_keywords):
        return False

    return True

# =========================================
# 4) 배치 히스토리 조회 (1년치)
# =========================================
def get_historical_data_batch(symbols: List[str], chunk_size=30) -> Dict[str, List[Dict]]:
    """
    여러 심볼을 묶어서 한 번에 1년치(252 거래일) 가격 데이터를 가져온다.
    returns: { symbol: [ {date, open, high, low, close, ...}, ... ], ... }
    """
    result = {}
    for i in range(0, len(symbols), chunk_size):
        chunk_symbols = symbols[i:i+chunk_size]
        joined_symbols = ",".join(chunk_symbols)
        url = (f"https://financialmodelingprep.com/api/v3/historical-price-full/"
               f"{joined_symbols}?timeseries=252&apikey={FMP_API_KEY}")
        # API 호출
        rate_limiter.wait_if_needed()
        try:
            resp = requests.get(url, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                # data는 {'symbol':..., 'historical':[...]}, 혹은 [{...}, {...}] 형태 가능
                # 여러 종목을 요청한 경우: data는 {"historicalStockList": [{symbol, historical[]}, ...]}
                #  -> 이 형태를 파싱해야 한다.
                if isinstance(data, dict) and "historicalStockList" in data:
                    for entry in data["historicalStockList"]:
                        sym = entry.get("symbol")
                        hist = entry.get("historical", [])
                        if hist:
                            result[sym] = sorted(hist, key=lambda x: x['date'], reverse=True)
                # 단일 종목 케이스 (심볼 1개만 조회했을 때)
                elif isinstance(data, dict) and "symbol" in data and "historical" in data:
                    sym = data["symbol"]
                    hist = data["historical"]
                    if hist:
                        result[sym] = sorted(hist, key=lambda x: x['date'], reverse=True)
                # 혹은 배열 형태로 반환될 수도 있음
                elif isinstance(data, list):
                    # list 형태: [{ 'symbol': 'AAPL', 'historical': [ ... ] }, {...}]
                    for item in data:
                        sym = item.get("symbol")
                        hist = item.get("historical", [])
                        if hist:
                            result[sym] = sorted(hist, key=lambda x: x['date'], reverse=True)
                else:
                    # 예외 형태
                    pass
            else:
                print(f"⚠️ 배치 히스토리 요청 에러({resp.status_code}): {joined_symbols}")
        except Exception as e:
            print(f"⚠️ 배치 히스토리 요청 실패: {joined_symbols} -> {e}")
    return result

# =========================================
# 5) RS(가중 1년 수익률) 계산
# =========================================
def calculate_weighted_return(historical: List[Dict]) -> float:
    """분기별 수익률 계산 + 가중치 적용"""
    if len(historical) < 252:
        return None

    # 필요한 인덱스(0, 63, 126, 189, 252)
    # historical는 내림차순(최신 -> 과거) 이므로
    # index=0 : 최신, index=63: 약 3개월전 ...
    needed_idx = [0, 63, 126, 189, 251]  # 252는 index가 0부터 시작이므로 251
    try:
        prices = {}
        for i in needed_idx:
            prices[i] = float(historical[i]['close'])
        # 분기별 수익률
        q1 = (prices[0] / prices[63] - 1)*100
        q2 = (prices[63] / prices[126] - 1)*100
        q3 = (prices[126] / prices[189] - 1)*100
        q4 = (prices[189] / prices[251] - 1)*100
        weighted_return = (q1*0.4 + q2*0.2 + q3*0.2 + q4*0.2)
        return weighted_return
    except Exception:
        return None

def calculate_rs_rating(returns_dict: Dict[str, float]) -> Dict[str, float]:
    """RS 등급 계산 (1-99 스케일)"""
    symbols = list(returns_dict.keys())
    rets = list(returns_dict.values())

    s = pd.Series(rets)
    ranks = s.rank(ascending=False)  # 높은 수익률일수록 rank=1
    n = len(rets)
    rs_values = ((n - ranks) / (n - 1) * 98) + 1
    rs_map = {}
    for sym, rs_v in zip(symbols, rs_values):
        rs_map[sym] = rs_v
    return rs_map

# =========================================
# 6) 52주 신고가 75% 이상 & 기술적 조건
# =========================================
def check_high_and_technical_conditions(
    symbol: str, 
    historical: List[Dict], 
    rs_value: float,
    rs_threshold=70,
    high_ratio=0.75
) -> bool:
    """
    - 현재가가 52주 고가의 75% 이상인가?
    - 50MA, 150MA, 200MA 위인지?
    - RS 70 이상?
    """
    if rs_value < rs_threshold:
        return False

    if not historical or len(historical) < 200:
        return False

    # 정렬: 최신 -> 과거
    closes = [safe_float(x.get('close')) for x in historical]
    current_price = closes[0]
    year_high = max(closes)  # 1년 최고가
    if year_high == 0 or (current_price / year_high) < high_ratio:
        return False

    # 이동평균 (단순예: 50일, 150일, 200일)
    # closes[0:50], closes[0:150], closes[0:200]
    ma50 = np.mean(closes[:50])
    ma150 = np.mean(closes[:150])
    ma200 = np.mean(closes[:200])

    # 간단한 조건 예시
    if not (current_price > ma50 > ma150 > ma200):
        return False

    return True

# =========================================
# 7) Airtable 업데이트
# =========================================
def update_airtable(stocks: List[Dict]):
    print("\n📡 Airtable 업데이트 시작...")
    airtable = Airtable(AIRTABLE_BASE_ID, TABLE_NAME, AIRTABLE_API_KEY)
    current_date = datetime.now().strftime("%Y-%m-%d")

    # 한번에 최대 10개씩 묶어서 전송 (Airtable 제한)
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

# =========================================
# 8) 메인 프로세스
# =========================================
def main():
    try:
        print("\n🚀 프로그램 시작...")
        print(f"FMP_API_KEY: {'설정됨' if FMP_API_KEY else '미설정'}")
        print(f"AIRTABLE_API_KEY: {'설정됨' if AIRTABLE_API_KEY else '미설정'}")
        print(f"AIRTABLE_BASE_ID: {'설정됨' if AIRTABLE_BASE_ID else '미설정'}")

        if not all([FMP_API_KEY, AIRTABLE_API_KEY, AIRTABLE_BASE_ID]):
            raise ValueError("필수 환경 변수가 설정되지 않았습니다.")

        print("🔎 종목 처리 시작...")

        # 1) 필터링용 기초 데이터
        delisted_stocks = get_delisted_stocks()
        tradable_stocks = get_tradable_stocks()

        # 2) NYSE, NASDAQ 모든 종목 데이터
        all_stocks = get_quotes()

        # 3) 유효한 종목만 추출
        valid_symbols = []
        symbol_to_data = {}
        for st in all_stocks:
            if is_valid_us_stock(st, delisted_stocks, tradable_stocks):
                symbol = st['symbol']
                valid_symbols.append(symbol)
                symbol_to_data[symbol] = st

        print(f"✔️ 사전 필터 후 종목 수: {len(valid_symbols)}개")

        # 4) 배치로 1년치 시세 가져오기
        hist_map = get_historical_data_batch(valid_symbols, chunk_size=30)
        print(f"✔️ 1년치 시세 확보 종목 수: {len(hist_map)}개")

        # 5) 종목별 가중 1년 수익률 계산
        returns_dict = {}
        for sym in hist_map:
            wret = calculate_weighted_return(hist_map[sym])
            if wret is not None:
                returns_dict[sym] = wret

        # RS 등급
        rs_dict = calculate_rs_rating(returns_dict)

        # 6) 52주 신고가 & 기술적 조건 체크
        filtered_stocks = []
        for sym, ret in rs_dict.items():
            hist = hist_map.get(sym, [])
            rs_val = ret
            if check_high_and_technical_conditions(sym, hist, rs_val):
                # Airtable에 넣을 데이터 구성
                stock_info = symbol_to_data[sym]
                current_price = safe_float(stock_info.get('price'))
                previous_close = safe_float(stock_info.get('previousClose'))
                year_high = max(safe_float(x['close']) for x in hist)
                price_to_high_ratio = 0
                if year_high > 0:
                    price_to_high_ratio = (current_price / year_high) * 100
                change_percent = 0
                if previous_close:
                    change_percent = ((current_price - previous_close) / previous_close) * 100

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

        # 7) Airtable 저장
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

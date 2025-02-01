import os
import requests
from datetime import datetime
from airtable import Airtable
import time
from typing import Dict, List, Optional, Tuple

POLYGON_API_KEY = "lsstdMdFXY50qjPNMQrXFp4vAGj0bNd5"
FMP_API_KEY = "EApxNJTRwcXOrhy2IUqSeKV0gyH8gans"
AIRTABLE_API_KEY = "patBy8FRWWiG6P99a.a0670e9dd25c84d028c9f708af81d5f1fb164c3adeb1cee067d100075db8b748"
AIRTABLE_BASE_ID = "appAh82iPV3cH6Xx5"
TABLE_NAME = "미국주식 데이터"

def is_valid_financials(financials: List) -> bool:
    """재무데이터의 기본적인 유효성을 검증"""
    if not financials:
        return False
    
    # 최소 5개 분기 데이터 필요 (전년 동기 비교를 위해)
    if len(financials) < 5:
        return False
            
    return True

def find_matching_quarter(current_date: str, past_quarters: List[Dict]) -> Optional[Dict]:
    """현재 분기에 대응하는 작년 동기 데이터를 찾음"""
    try:
        current = datetime.strptime(current_date, '%Y-%m-%d')
        target_month = current.month
        
        for quarter in past_quarters:
            past_date = datetime.strptime(quarter['date'], '%Y-%m-%d')
            if past_date.month == target_month and past_date.year == current.year - 1:
                return quarter
    except Exception as e:
        print(f"분기 매칭 중 에러 발생: {str(e)}")
    
    return None

def safe_growth_rate(current: float, previous: float) -> Optional[float]:
    """안전하게 성장률을 계산"""
    try:
        if current is None or previous is None:
            return None
            
        current = float(current)
        previous = float(previous)
        
        if previous == 0:
            return None
            
        return ((current - previous) / abs(previous)) * 100
            
    except (ValueError, TypeError) as e:
        print(f"성장률 계산 중 에러: {str(e)}")
        return None

def get_financials_fmp(ticker: str) -> List:
    """FMP API를 사용하여 재무데이터 조회"""
    url = f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}"
    params = {
        'apikey': FMP_API_KEY,
        'period': 'quarter',
        'limit': 20  # 5년치 분기 데이터
    }
    try:
        print(f"\n재무데이터 요청: {ticker}")
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            financials = response.json()
            print(f"재무데이터 수신 성공: {ticker} (데이터 수: {len(financials)})")
            
            if not financials:
                print(f"재무데이터 없음: {ticker}")
                return []
                
            # 날짜 기준 내림차순 정렬 (최신 순)
            return sorted(financials, key=lambda x: x.get('date', ''), reverse=True)
        else:
            print(f"재무데이터 조회 실패 ({ticker}): {response.status_code}")
            print(f"응답 내용: {response.text}")
            return []
    except Exception as e:
        print(f"재무데이터 조회 중 에러 발생 ({ticker}): {str(e)}")
        return []

def calculate_growth_rates_fmp(financials: List) -> Dict:
    """재무 성장률 계산 (전년 동기 대비)"""
    growth_rates = {
        'eps_growth': {'q1': None, 'q2': None, 'q3': None, 'y1': None, 'y2': None, 'y3': None},
        'operating_income_growth': {'q1': None, 'q2': None, 'q3': None, 'y1': None, 'y2': None, 'y3': None},
        'revenue_growth': {'q1': None, 'q2': None, 'q3': None, 'y1': None, 'y2': None, 'y3': None}
    }
    
    if not is_valid_financials(financials):
        print("유효하지 않은 재무데이터")
        return growth_rates

    try:
        # 분기별 성장률 계산
        for i in range(3):  # 최근 3개 분기
            if i >= len(financials):
                break
                
            current_quarter = financials[i]
            past_quarters = financials[4:]  # 1년 이상 지난 데이터
            
            year_ago_quarter = find_matching_quarter(current_quarter['date'], past_quarters)
            if year_ago_quarter:
                # EPS 성장률
                current_eps = current_quarter.get('eps', 0)
                previous_eps = year_ago_quarter.get('eps', 0)
                
                # 영업이익 성장률
                current_op = current_quarter.get('operatingIncome', 0)
                previous_op = year_ago_quarter.get('operatingIncome', 0)
                
                # 매출액 성장률
                current_rev = current_quarter.get('revenue', 0)
                previous_rev = year_ago_quarter.get('revenue', 0)
                
                quarter_key = f'q{i+1}'
                growth_rates['eps_growth'][quarter_key] = safe_growth_rate(current_eps, previous_eps)
                growth_rates['operating_income_growth'][quarter_key] = safe_growth_rate(current_op, previous_op)
                growth_rates['revenue_growth'][quarter_key] = safe_growth_rate(current_rev, previous_rev)

        # 연간 재무 데이터 집계
        def get_annual_data(quarters: List[Dict]) -> Dict:
            return {
                'eps': sum(float(q.get('eps', 0) or 0) for q in quarters),
                'operatingIncome': sum(float(q.get('operatingIncome', 0) or 0) for q in quarters),
                'revenue': sum(float(q.get('revenue', 0) or 0) for q in quarters)
            }

        annual_data = []
        for year in range(4):  # 최근 4년
            start_idx = year * 4
            if start_idx + 3 < len(financials):
                year_quarters = financials[start_idx:start_idx + 4]
                annual_data.append(get_annual_data(year_quarters))

        # 연간 성장률 계산
        for i in range(3):  # 3년간의 성장률
            if i + 1 < len(annual_data):
                current = annual_data[i]
                previous = annual_data[i + 1]
                
                year_key = f'y{i+1}'
                growth_rates['eps_growth'][year_key] = safe_growth_rate(current['eps'], previous['eps'])
                growth_rates['operating_income_growth'][year_key] = safe_growth_rate(
                    current['operatingIncome'],
                    previous['operatingIncome']
                )
                growth_rates['revenue_growth'][year_key] = safe_growth_rate(
                    current['revenue'],
                    previous['revenue']
                )

        # 계산된 성장률 출력
        print("\n성장률 계산 결과:")
        for metric, periods in growth_rates.items():
            print(f"\n{metric}:")
            for period, value in periods.items():
                print(f"{period}: {value:.1f}%" if value is not None else f"{period}: None")
                
    except Exception as e:
        print(f"성장률 계산 중 에러 발생: {str(e)}")
    
    return growth_rates

def update_airtable(stock_data: List, category: str):
    """Airtable에 데이터 업데이트"""
    airtable = Airtable(AIRTABLE_BASE_ID, TABLE_NAME, AIRTABLE_API_KEY)
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    for stock in stock_data:
        try:
            print(f"\n{stock['ticker']} 처리 중...")
            
            financials = get_financials_fmp(stock['ticker'])
            if not financials:
                print(f"재무데이터 부족으로 건너뛰기: {stock['ticker']}")
                continue
            
            # 데이터 디버깅
            print(f"\n최근 4분기 데이터:")
            for q in financials[:4]:
                print(f"날짜: {q.get('date')}, EPS: {q.get('eps')}, 영업이익: {q.get('operatingIncome')}, 매출: {q.get('revenue')}")
                
            growth_rates = calculate_growth_rates_fmp(financials)
            
            record = {
                '티커': stock.get('ticker', ''),
                '종목명': stock.get('name', ''),
                '현재가': float(stock.get('day', {}).get('c', 0)),
                '등락률': float(stock.get('todaysChangePerc', 0)),
                '거래량': int(stock.get('day', {}).get('v', 0)),
                '시가총액': float(stock.get('market_cap', 0)),
                '업데이트 시간': current_date,
                '분류': category,
                'EPS성장률_최신분기': growth_rates['eps_growth']['q1'],
                'EPS성장률_전분기': growth_rates['eps_growth']['q2'],
                'EPS성장률_전전분기': growth_rates['eps_growth']['q3'],
                'EPS성장률_1년': growth_rates['eps_growth']['y1'],
                'EPS성장률_2년': growth_rates['eps_growth']['y2'],
                'EPS성장률_3년': growth_rates['eps_growth']['y3'],
                '영업이익성장률_최신분기': growth_rates['operating_income_growth']['q1'],
                '영업이익성장률_전분기': growth_rates['operating_income_growth']['q2'],
                '영업이익성장률_전전분기': growth_rates['operating_income_growth']['q3'],
                '영업이익성장률_1년': growth_rates['operating_income_growth']['y1'],
                '영업이익성장률_2년': growth_rates['operating_income_growth']['y2'],
                '영업이익성장률_3년': growth_rates['operating_income_growth']['y3'],
                '매출액성장률_최신분기': growth_rates['revenue_growth']['q1'],
                '매출액성장률_전분기': growth_rates['revenue_growth']['q2'],
                '매출액성장률_전전분기': growth_rates['revenue_growth']['q3'],
                '매출액성장률_1년': growth_rates['revenue_growth']['y1'],
                '매출액성장률_2년': growth_rates['revenue_growth']['y2'],
                '매출액성장률_3년': growth_rates['revenue_growth']['y3'],
            }
            
            if stock.get('primary_exchange'):
                record['거래소 정보'] = convert_exchange_code(stock['primary_exchange'])
            
            airtable.insert(record)
            print(f"데이터 추가 완료: {record['티커']}")
            time.sleep(1)  # Rate limit 고려
                
        except Exception as e:
            print(f"레코드 처리 중 에러 발생 ({stock.get('ticker', 'Unknown')}): {str(e)}")

def get_stock_details(ticker: str) -> Dict:
    """Polygon API를 사용하여 주식 기본 정보 조회"""
    url = f"https://api.polygon.io/v3/reference/tickers/{ticker}"
    params = {'apiKey': POLYGON_API_KEY}
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response.json().get('results', {})
        else:
            print(f"종목 상세정보 조회 실패 ({ticker}): {response.status_code}")
            print(f"응답 내용: {response.text}")
            return None
    except Exception as e:
        print(f"종목 상세정보 조회 중 에러 발생 ({ticker}): {str(e)}")
        return None

def get_all_stocks():
    """Polygon API를 사용하여 모든 주식 데이터 조회"""
    url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
    params = {
        'apiKey': POLYGON_API_KEY,
        'include_otc': False
    }
    
    try:
        print(f"API 요청 시작: {url}")
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            return data.get('tickers', [])
        else:
            print(f"API 요청 실패: {response.status_code}")
            print(f"응답 내용: {response.text}")
            return []
            
    except Exception as e:
        print(f"데이터 수집 중 에러 발생: {str(e)}")
        return []

def filter_stocks(stocks: List) -> List:
    """주식 데이터 필터링"""
    filtered = []
    total = len(stocks)
    
    print(f"총 {total}개 종목 필터링 시작...")
    
    for i, stock in enumerate(stocks, 1):
        try:
            day_data = stock.get('day', {})
            price = float(day_data.get('c', 0))
            volume = int(day_data.get('v', 0))
            change = float(stock.get('todaysChangePerc', 0))
            
            if i % 100 == 0:
                print(f"진행 중... {i}/{total}")
            
            if price >= 5 and volume >= 1000000 and change >= 5:
                details = get_stock_details(stock['ticker'])
                if details:
                    market_cap = float(details.get('market_cap', 0))
                    if market_cap >= 500000000:
                        stock['name'] = details.get('name', '')
                        stock['market_cap'] = market_cap
                        stock['primary_exchange'] = details.get('primary_exchange', '')
                        filtered.append(stock)
                        print(f"조건 만족: {stock['ticker']} (시가총액: ${market_cap:,.2f})")
                        time.sleep(0.5)  # Rate limit 고려
        
        except Exception as e:
            print(f"종목 필터링 중 에러 발생 ({stock.get('ticker', 'Unknown')}): {str(e)}")
            continue
    
    return sorted(filtered, key=lambda x: x.get('todaysChangePerc', 0), reverse=True)

def convert_exchange_code(mic: str) -> str:
    """거래소 코드 변환"""
    exchange_map = {
        'XNAS': 'NASDAQ',
        'XNYS': 'NYSE',
        'XASE': 'AMEX'
    }
    return exchange_map.get(mic, mic)

def main():
    print("데이터 수집 시작...")
    print("필터링 조건: 현재가 >= $5, 거래량 >= 1M주, 등락률 >= 5%, 시가총액 >= $500M")
    
    all_stocks = get_all_stocks()
    if not all_stocks:
        print("데이터 수집 실패")
        return
        
    print(f"\n총 {len(all_stocks)}개 종목 데이터 수집됨")
    filtered_stocks = filter_stocks(all_stocks)
    print(f"\n조건을 만족하는 종목 수: {len(filtered_stocks)}개")
    
    if filtered_stocks:
        update_airtable(filtered_stocks, "전일대비등락률상위")
    
    print("\n모든 데이터 처리 완료!")

if __name__ == "__main__":
    main()

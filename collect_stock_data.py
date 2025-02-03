import os
import requests
from datetime import datetime
from airtable import Airtable
import time
from typing import Dict, List, Optional, Tuple

# API 키들을 환경변수에서 가져오기
POLYGON_API_KEY = os.getenv('POLYGON_API_KEY')
FMP_API_KEY = os.getenv('FMP_API_KEY')
AIRTABLE_API_KEY = os.getenv('AIRTABLE_API_KEY')
AIRTABLE_BASE_ID = os.getenv('AIRTABLE_BASE_ID')
TABLE_NAME = "미국주식 데이터"

# API 키 유효성 검사
if not all([POLYGON_API_KEY, FMP_API_KEY, AIRTABLE_API_KEY, AIRTABLE_BASE_ID]):
    raise ValueError("필요한 API 키가 환경변수에 설정되지 않았습니다.")

def calculate_eps(net_income: float, shares: float) -> Optional[float]:
    """순이익과 주식수로 EPS 직접 계산"""
    try:
        if not net_income or not shares or shares <= 0:
            return None
        return net_income / shares
    except (ValueError, TypeError, ZeroDivisionError) as e:
        print(f"EPS 계산 중 에러: {str(e)}")
        return None

def find_matching_quarter_data(current_data: Dict, financials: List[Dict]) -> Optional[Dict]:
    """Calendar Year와 Period 기준으로 전년 동기 데이터 찾기"""
    try:
        current_year = int(current_data.get('calendarYear', 0))
        current_period = current_data.get('period', '')
        target_year = current_year - 1
        
        for quarter in financials:
            if (int(quarter.get('calendarYear', 0)) == target_year and 
                quarter.get('period', '') == current_period):
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

def get_financials_fmp(ticker: str, period: str = 'quarter') -> List:
    """FMP API를 사용하여 재무데이터 조회"""
    url = f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}"
    params = {
        'apikey': FMP_API_KEY,
        'period': period,
        'limit': 20 if period == 'quarter' else 5
    }
    
    try:
        print(f"\n재무데이터 요청: {ticker} ({period})")
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            financials = response.json()
            print(f"재무데이터 수신 성공: {ticker} (데이터 수: {len(financials)})")
            
            if not financials:
                print(f"재무데이터 없음: {ticker}")
                return []
                
            return sorted(financials, key=lambda x: x.get('date', ''), reverse=True)
        else:
            print(f"재무데이터 조회 실패 ({ticker}): {response.status_code}")
            return []
    except Exception as e:
        print(f"재무데이터 조회 중 에러 발생 ({ticker}): {str(e)}")
        return []

def calculate_growth_rates_fmp(ticker: str) -> Dict:
    """재무 성장률 계산 (전년 동기 대비)"""
    growth_rates = {
        'dates': {
            'quarters': {'q1': None, 'q2': None, 'q3': None},
            'years': {'y1': None, 'y2': None, 'y3': None}
        },
        'eps_growth': {'q1': None, 'q2': None, 'q3': None, 'y1': None, 'y2': None, 'y3': None},
        'operating_income_growth': {'q1': None, 'q2': None, 'q3': None, 'y1': None, 'y2': None, 'y3': None},
        'revenue_growth': {'q1': None, 'q2': None, 'q3': None, 'y1': None, 'y2': None, 'y3': None}
    }
    
    # 분기 데이터 조회
    quarterly_data = get_financials_fmp(ticker, 'quarter')
    if not quarterly_data:
        return growth_rates
        
    # 연간 데이터 조회
    annual_data = get_financials_fmp(ticker, 'annual')
    if not annual_data:
        return growth_rates

    try:
        # 분기별 성장률 계산
        for i in range(min(3, len(quarterly_data))):
            current_quarter = quarterly_data[i]
            year_ago_quarter = find_matching_quarter_data(current_quarter, quarterly_data)
            
            if year_ago_quarter:
                quarter_key = f'q{i+1}'
                # 날짜 정보로 full date 저장 (YYYY-MM-DD)
                growth_rates['dates']['quarters'][quarter_key] = current_quarter['date']
                
                # EPS 계산
                current_eps = calculate_eps(
                    current_quarter.get('netIncome', 0),
                    current_quarter.get('weightedAverageShsOut', 0)
                )
                previous_eps = calculate_eps(
                    year_ago_quarter.get('netIncome', 0),
                    year_ago_quarter.get('weightedAverageShsOut', 0)
                )
                
                # 성장률 계산
                growth_rates['eps_growth'][quarter_key] = safe_growth_rate(current_eps, previous_eps)
                growth_rates['operating_income_growth'][quarter_key] = safe_growth_rate(
                    current_quarter.get('operatingIncome'),
                    year_ago_quarter.get('operatingIncome')
                )
                growth_rates['revenue_growth'][quarter_key] = safe_growth_rate(
                    current_quarter.get('revenue'),
                    year_ago_quarter.get('revenue')
                )

        # 연간 성장률 계산
        for i in range(min(3, len(annual_data) - 1)):
            current_year = annual_data[i]
            previous_year = annual_data[i + 1]
            
            year_key = f'y{i+1}'
            # 연간 데이터는 Calendar Year만 저장
            growth_rates['dates']['years'][year_key] = current_year['calendarYear']
            
            # EPS 계산
            current_eps = calculate_eps(
                current_year.get('netIncome', 0),
                current_year.get('weightedAverageShsOut', 0)
            )
            previous_eps = calculate_eps(
                previous_year.get('netIncome', 0),
                previous_year.get('weightedAverageShsOut', 0)
            )
            
            # 성장률 계산
            growth_rates['eps_growth'][year_key] = safe_growth_rate(current_eps, previous_eps)
            growth_rates['operating_income_growth'][year_key] = safe_growth_rate(
                current_year.get('operatingIncome'),
                previous_year.get('operatingIncome')
            )
            growth_rates['revenue_growth'][year_key] = safe_growth_rate(
                current_year.get('revenue'),
                previous_year.get('revenue')
            )
                
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
            
            growth_rates = calculate_growth_rates_fmp(stock['ticker'])
            
            record = {
                '티커': stock.get('ticker', ''),
                '종목명': stock.get('name', ''),
                '현재가': float(stock.get('day', {}).get('c', 0)),
                '등락률': float(stock.get('todaysChangePerc', 0)),
                '거래량': int(stock.get('day', {}).get('v', 0)),
                '시가총액': float(stock.get('market_cap', 0)),
                '업데이트 시간': current_date,
                '분류': category,
                
                # EPS 성장률과 날짜
                'EPS성장률_최신분기': growth_rates['eps_growth']['q1'],
                'EPS성장률_전분기': growth_rates['eps_growth']['q2'],
                'EPS성장률_전전분기': growth_rates['eps_growth']['q3'],
                'EPS성장률_1년': growth_rates['eps_growth']['y1'],
                'EPS성장률_2년': growth_rates['eps_growth']['y2'],
                'EPS성장률_3년': growth_rates['eps_growth']['y3'],
                
                'EPS성장률_최신분기(날짜)': growth_rates['dates']['quarters'].get('q1'),
                'EPS성장률_전분기(날짜)': growth_rates['dates']['quarters'].get('q2'),
                'EPS성장률_전전분기(날짜)': growth_rates['dates']['quarters'].get('q3'),
                'EPS성장률_1년(날짜)': growth_rates['dates']['years'].get('y1'),
                'EPS성장률_2년(날짜)': growth_rates['dates']['years'].get('y2'),
                'EPS성장률_3년(날짜)': growth_rates['dates']['years'].get('y3'),
                
                # 영업이익 성장률과 날짜
                '영업이익성장률_최신분기': growth_rates['operating_income_growth']['q1'],
                '영업이익성장률_전분기': growth_rates['operating_income_growth']['q2'],
                '영업이익성장률_전전분기': growth_rates['operating_income_growth']['q3'],
                '영업이익성장률_1년': growth_rates['operating_income_growth']['y1'],
                '영업이익성장률_2년': growth_rates['operating_income_growth']['y2'],
                '영업이익성장률_3년': growth_rates['operating_income_growth']['y3'],
                
                '영업이익성장률_최신분기(날짜)': growth_rates['dates']['quarters'].get('q1'),
                '영업이익성장률_전분기(날짜)': growth_rates['dates']['quarters'].get('q2'),
                '영업이익성장률_전전분기(날짜)': growth_rates['dates']['quarters'].get('q3'),
                '영업이익성장률_1년(날짜)': growth_rates['dates']['years'].get('y1'),
                '영업이익성장률_2년(날짜)': growth_rates['dates']['years'].get('y2'),
                '영업이익성장률_3년(날짜)': growth_rates['dates']['years'].get('y3'),
                
                # 매출액 성장률과 날짜
                '매출액성장률_최신분기': growth_rates['revenue_growth']['q1'],
                '매출액성장률_전분기': growth_rates['revenue_growth']['q2'],
                '매출액성장률_전전분기': growth_rates['revenue_growth']['q3'],
                '매출액성장률_1년': growth_rates['revenue_growth']['y1'],
                '매출액성장률_2년': growth_rates['revenue_growth']['y2'],
                '매출액성장률_3년': growth_rates['revenue_growth']['y3'],
                
                '매출액성장률_최신분기(날짜)': growth_rates['dates']['quarters'].get('q1'),
                '매출액성장률_전분기(날짜)': growth_rates['dates']['quarters'].get('q2'),
                '매출액성장률_전전분기(날짜)': growth_rates['dates']['quarters'].get('q3'),
                '매출액성장률_1년(날짜)': growth_rates['dates']['years'].get('y1'),
                '매출액성장률_2년(날짜)': growth_rates['dates']['years'].get('y2'),
                '매출액성장률_3년(날짜)': growth_rates['dates']['years'].get('y3'),
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
    try:
        print("데이터 수집 시작...")
        print("필터링 조건: 현재가 >= $5, 거래량 >= 1M주, 등락률 >= 5%, 시가총액 >= $500M")
        
        # Polygon API로 전일대비 상승률 상위 종목 필터링
        all_stocks = get_all_stocks()
        if not all_stocks:
            print("데이터 수집 실패")
            return
            
        print(f"\n총 {len(all_stocks)}개 종목 데이터 수집됨")
        filtered_stocks = filter_stocks(all_stocks)
        print(f"\n조건을 만족하는 종목 수: {len(filtered_stocks)}개")
        
        if filtered_stocks:
            # FMP API로 재무데이터 조회 및 Airtable 업데이트
            update_airtable(filtered_stocks, "전일대비등락률상위")
        
        print("\n모든 데이터 처리 완료!")
        
    except Exception as e:
        print(f"프로그램 실행 중 오류 발생: {str(e)}")

if __name__ == "__main__":
    main()

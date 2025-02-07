import os
import requests
from datetime import datetime
from airtable import Airtable
import time
from typing import Dict, List, Optional, Tuple

# API 키 설정
POLYGON_API_KEY = "lsstdMdFXY50qjPNMQrXFp4vAGj0bNd5"
FMP_API_KEY = "EApxNJTRwcXOrhy2IUqSeKV0gyH8gans"
AIRTABLE_API_KEY = "patBy8FRWWiG6P99a.a0670e9dd25c84d028c9f708af81d5f1fb164c3adeb1cee067d100075db8b748"

# Airtable 설정
SOURCE_BASE_ID = "appAh82iPV3cH6Xx5"  # 동일한 베이스를 사용
TARGET_BASE_ID = "appAh82iPV3cH6Xx5"
SOURCE_TABLE_NAME = "트레이더의 선택"
SOURCE_VIEW_NAME = "마크미너비니"
TARGET_TABLE_NAME = "미국주식 데이터"

def get_tickers_from_airtable() -> List[str]:
    """Airtable에서 티커 목록 가져오기"""
    try:
        airtable = Airtable(SOURCE_BASE_ID, SOURCE_TABLE_NAME, AIRTABLE_API_KEY)
        records = airtable.get_all(view=SOURCE_VIEW_NAME)
        
        tickers = []
        for record in records:
            ticker = record['fields'].get('티커')
            if ticker:
                tickers.append(ticker)
                
        print(f"Airtable에서 {len(tickers)}개의 티커를 불러왔습니다.")
        return tickers
        
    except Exception as e:
        print(f"티커 목록 조회 중 에러 발생: {str(e)}")
        return []

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

def get_stock_data(ticker: str) -> Dict:
    """Polygon API를 사용하여 주식 데이터 조회"""
    url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}"
    params = {'apiKey': POLYGON_API_KEY}
    
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            if data.get('status') == 'OK':
                return data.get('ticker', {})
        print(f"주식 데이터 조회 실패 ({ticker}): {response.status_code}")
        return {}
    except Exception as e:
        print(f"주식 데이터 조회 중 에러 발생 ({ticker}): {str(e)}")
        return {}

def get_stock_details(ticker: str) -> Dict:
    """Polygon API를 사용하여 주식 상세 정보 조회"""
    url = f"https://api.polygon.io/v3/reference/tickers/{ticker}"
    params = {'apiKey': POLYGON_API_KEY}
    
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response.json().get('results', {})
        print(f"종목 상세정보 조회 실패 ({ticker}): {response.status_code}")
        return {}
    except Exception as e:
        print(f"종목 상세정보 조회 중 에러 발생 ({ticker}): {str(e)}")
        return {}

def update_airtable(stock_data: List, category: str):
    """Airtable에 데이터 업데이트"""
    airtable = Airtable(TARGET_BASE_ID, TARGET_TABLE_NAME, AIRTABLE_API_KEY)
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
        
        # Airtable에서 티커 목록 가져오기
        tickers = get_tickers_from_airtable()
        if not tickers:
            print("티커 목록을 가져오지 못했습니다.")
            return
            
        stock_data = []
        for ticker in tickers:
            try:
                print(f"\n{ticker} 데이터 수집 중...")
                
                # 기본 주식 데이터 조회
                data = get_stock_data(ticker)
                if not data:
                    continue
                    
                # 상세 정보 조회 및 데이터 병합
                details = get_stock_details(ticker)
                if details:
                    data['name'] = details.get('name', '')
                    data['market_cap'] = float(details.get('market_cap', 0))
                    data['primary_exchange'] = details.get('primary_exchange', '')
                    
                stock_data.append(data)
                time.sleep(0.5)  # Rate limit 고려
                
            except Exception as e:
                print(f"{ticker} 처리 중 에러 발생: {str(e)}")
                continue
        
        # Airtable 업데이트
        if stock_data:
            update_airtable(stock_data, "마크미너비니")
            print(f"\n{len(stock_data)}개 종목의 데이터 처리 완료!")
        else:
            print("\n처리할 데이터가 없습니다.")
            
    except Exception as e:
        print(f"프로그램 실행 중 오류 발생: {str(e)}")

if __name__ == "__main__":
    main()

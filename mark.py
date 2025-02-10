import os
import requests
from datetime import datetime
from airtable import Airtable
import time
from typing import Dict, List, Optional, Tuple

# API 키 직접 설정
POLYGON_API_KEY = "lsstdMdFXY50qjPNMQrXFp4vAGj0bNd5"
FMP_API_KEY = "EApxNJTRwcXOrhy2IUqSeKV0gyH8gans"
AIRTABLE_API_KEY = "patBy8FRWWiG6P99a.a0670e9dd25c84d028c9f708af81d5f1fb164c3adeb1cee067d100075db8b748"
AIRTABLE_BASE_ID = "appAh82iPV3cH6Xx5"
SOURCE_TABLE_NAME = "트레이더의 선택"
SOURCE_VIEW_NAME = "마크미너비니"
TARGET_TABLE_NAME = "미국주식 데이터"

def get_tickers_from_airtable() -> List[str]:
    """Airtable에서 티커 목록 가져오기"""
    try:
        airtable = Airtable(AIRTABLE_BASE_ID, SOURCE_TABLE_NAME, AIRTABLE_API_KEY)
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

[기존 코드의 나머지 함수들 유지...]

def get_stock_data(tickers: List[str]) -> List[Dict]:
    """Polygon API를 사용하여 특정 티커들의 주식 데이터 조회"""
    url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
    params = {
        'apiKey': POLYGON_API_KEY,
        'tickers': ','.join(tickers)  # 콤마로 구분된 티커 목록
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

def update_airtable(stock_data: List, category: str):
    """Airtable에 데이터 업데이트"""
    airtable = Airtable(AIRTABLE_BASE_ID, TARGET_TABLE_NAME, AIRTABLE_API_KEY)
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    # 기존 레코드 조회
    existing_records = {}
    records = airtable.get_all()
    for record in records:
        ticker = record['fields'].get('티커')
        if ticker:
            existing_records[ticker] = record['id']
    
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
            
            # 기존 레코드가 있으면 업데이트, 없으면 새로 추가
            ticker = stock['ticker']
            if ticker in existing_records:
                airtable.update(existing_records[ticker], record)
                print(f"데이터 업데이트 완료: {ticker}")
            else:
                airtable.insert(record)
                print(f"새로운 데이터 추가 완료: {ticker}")
                
            time.sleep(1)  # Rate limit 고려
                
        except Exception as e:
            print(f"레코드 처리 중 에러 발생 ({stock.get('ticker', 'Unknown')}): {str(e)}")

def main():
    try:
        print("데이터 수집 시작...")
        
        # Airtable에서 티커 목록 가져오기
        tickers = get_tickers_from_airtable()
        if not tickers:
            print("티커 목록을 가져오지 못했습니다.")
            return
            
        # 티커들의 주식 데이터 조회
        stock_data = []
        for ticker in tickers:
            try:
                data = get_stock_data([ticker])  # 리스트로 전달
                if data and data[0]:  # 첫 번째 결과만 사용
                    details = get_stock_details(ticker)
                    if details:
                        data[0]['name'] = details.get('name', '')
                        data[0]['market_cap'] = float(details.get('market_cap', 0))
                        data[0]['primary_exchange'] = details.get('primary_exchange', '')
                        stock_data.append(data[0])
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

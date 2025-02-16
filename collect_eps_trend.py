import requests
from bs4 import BeautifulSoup
from airtable import Airtable
import time
from typing import Dict, List
from datetime import datetime

# === Airtable 설정 ===
AIRTABLE_API_KEY = "patBy8FRWWiG6P99a.a0670e9dd25c84d028c9f708af81d5f1fb164c3adeb1cee067d100075db8b748"
SOURCE_BASE_ID = "appAh82iPV3cH6Xx5"  # 동일한 베이스를 사용
TARGET_BASE_ID = "appAh82iPV3cH6Xx5"
SOURCE_TABLE_NAME = "트레이더의 선택"
SOURCE_VIEW_NAME = "마크미너비니"
TARGET_TABLE_NAME = "트레이더의 선택"

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

def get_eps_trend_data(symbol: str) -> Dict:
   """Yahoo Finance에서 EPS Trend 데이터 스크래핑"""
   try:
       url = f'https://finance.yahoo.com/quote/{symbol}/analysis'
       headers = {
           'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
       }
       
       print(f"Fetching EPS Trend data for {symbol}...")
       response = requests.get(url, headers=headers)
       
       if response.status_code == 200:
           soup = BeautifulSoup(response.text, 'html.parser')
           tables = soup.find_all('table')
           
           if len(tables) >= 4:
               eps_trend_table = tables[3]
               rows = eps_trend_table.find_all('tr')
               
               data = {
                   # Current Qtr
                   '현재분기 현재추정': None, '현재분기 7일전': None,
                   '현재분기 30일전': None, '현재분기 60일전': None,
                   '현재분기 90일전': None,
                   # Next Qtr
                   '다음분기 현재추정': None, '다음분기 7일전': None,
                   '다음분기 30일전': None, '다음분기 60일전': None,
                   '다음분기 90일전': None,
                   # Current Year
                   '현재연도 현재추정': None, '현재연도 7일전': None,
                   '현재연도 30일전': None, '현재연도 60일전': None,
                   '현재연도 90일전': None,
                   # Next Year
                   '내년 현재추정': None, '내년 7일전': None,
                   '내년 30일전': None, '내년 60일전': None,
                   '내년 90일전': None,
                   # 업데이트 시간 추가
                   '업데이트 시간': datetime.now().strftime("%Y-%m-%d")
               }
               
               # 데이터 추출
               for i, row in enumerate(rows):
                   if i == 0:  # 헤더 행 스킵
                       continue
                   
                   cells = row.find_all(['td', 'th'])
                   if len(cells) >= 5:  # 데이터 행
                       values = [cell.text.strip() for cell in cells[1:]]  # 첫 번째 열은 레이블
                       
                       if i == 1:  # Current Estimate
                           data['현재분기 현재추정'] = values[0]
                           data['다음분기 현재추정'] = values[1]
                           data['현재연도 현재추정'] = values[2]
                           data['내년 현재추정'] = values[3]
                       elif i == 2:  # 7 Days Ago
                           data['현재분기 7일전'] = values[0]
                           data['다음분기 7일전'] = values[1]
                           data['현재연도 7일전'] = values[2]
                           data['내년 7일전'] = values[3]
                       elif i == 3:  # 30 Days Ago
                           data['현재분기 30일전'] = values[0]
                           data['다음분기 30일전'] = values[1]
                           data['현재연도 30일전'] = values[2]
                           data['내년 30일전'] = values[3]
                       elif i == 4:  # 60 Days Ago
                           data['현재분기 60일전'] = values[0]
                           data['다음분기 60일전'] = values[1]
                           data['현재연도 60일전'] = values[2]
                           data['내년 60일전'] = values[3]
                       elif i == 5:  # 90 Days Ago
                           data['현재분기 90일전'] = values[0]
                           data['다음분기 90일전'] = values[1]
                           data['현재연도 90일전'] = values[2]
                           data['내년 90일전'] = values[3]
               
               print(f"Data collected for {symbol}")
               return data
           
           print(f"Required table not found for {symbol}")
           return {}
           
       else:
           print(f"Failed to fetch data for {symbol}. Status code: {response.status_code}")
           return {}
           
   except Exception as e:
       print(f"Error fetching data for {symbol}: {e}")
       return {}

def update_airtable(data: Dict, ticker: str):
    """Airtable 데이터 업데이트"""
    try:
        airtable = Airtable(TARGET_BASE_ID, TARGET_TABLE_NAME, AIRTABLE_API_KEY)
        
        # 데이터 정리
        record = {
            '티커': ticker,
            '업데이트 시간': data['업데이트 시간'],
            
            # Current Qtr
            '현재분기 현재추정': data['현재분기 현재추정'],
            '현재분기 7일전': data['현재분기 7일전'],
            '현재분기 30일전': data['현재분기 30일전'],
            '현재분기 60일전': data['현재분기 60일전'],
            '현재분기 90일전': data['현재분기 90일전'],
            
            # Next Qtr
            '다음분기 현재추정': data['다음분기 현재추정'],
            '다음분기 7일전': data['다음분기 7일전'],
            '다음분기 30일전': data['다음분기 30일전'],
            '다음분기 60일전': data['다음분기 60일전'],
            '다음분기 90일전': data['다음분기 90일전'],
            
            # Current Year
            '현재연도 현재추정': data['현재연도 현재추정'],
            '현재연도 7일전': data['현재연도 7일전'],
            '현재연도 30일전': data['현재연도 30일전'],
            '현재연도 60일전': data['현재연도 60일전'],
            '현재연도 90일전': data['현재연도 90일전'],
            
            # Next Year
            '내년 현재추정': data['내년 현재추정'],
            '내년 7일전': data['내년 7일전'],
            '내년 30일전': data['내년 30일전'],
            '내년 60일전': data['내년 60일전'],
            '내년 90일전': data['내년 90일전']
        }
        
        # 해당 티커의 레코드 찾기 (마크미너비니 뷰에서)
        existing_records = airtable.search('티커', ticker, view=마크미너비니)
        
        if existing_records:
            record_id = existing_records[0]['id']
            airtable.update(record_id, record)
            print(f"{ticker} 데이터 업데이트 완료")
        else:
            airtable.insert(record)
            print(f"{ticker} 신규 데이터 입력 완료")
            
    except Exception as e:
        print(f"Airtable 업데이트 중 에러 발생 ({ticker}): {e}")

def main():
   try:
       print("데이터 수집 시작...")
       
       # Airtable에서 티커 목록 가져오기
       tickers = get_tickers_from_airtable()
       if not tickers:
           print("티커 목록을 가져오지 못했습니다.")
           return
           
       for ticker in tickers:
           try:
               print(f"\n{ticker} 처리 중...")
               
               # EPS Trend 데이터 가져오기
               eps_data = get_eps_trend_data(ticker)
               
               if eps_data:
                   # Airtable 업데이트
                   update_airtable(eps_data, ticker)
               else:
                   print(f"{ticker}의 데이터를 가져오지 못했습니다.")
                   
               # Rate limiting
               time.sleep(2)  # 요청 간격 2초
               
           except Exception as e:
               print(f"{ticker} 처리 중 에러 발생: {e}")
               continue
               
       print("\n모든 데이터 처리 완료!")
       
   except Exception as e:
       print(f"프로그램 실행 중 오류 발생: {e}")

if __name__ == "__main__":
   main()

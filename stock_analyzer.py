import concurrent.futures
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pytz import timezone
from typing import Dict, List, Tuple
from airtable import Airtable
import os
import requests
import FinanceDataReader as fdr
import time
from random import uniform

class StockAnalyzer:
    def __init__(self):
        self.kst = timezone('Asia/Seoul')
        self.base_id = 'appJFk54sIT9oSiZy'
        self.table_name = '마크미너비니'
        self.api_key = os.environ['AIRTABLE_API_KEY']
        self.airtable = Airtable(self.base_id, self.table_name, self.api_key)
        
        self.today = datetime.now(self.kst)
        self.start_date = (self.today - timedelta(days=280)).strftime('%Y-%m-%d')
        self.end_date = self.today.strftime('%Y-%m-%d')
        
        # 동시 실행 수 제한
        self.max_workers = 5

    def get_stock_data_with_return(self, ticker: str) -> Tuple[str, pd.DataFrame, float]:
        """병렬 처리를 위한 데이터 및 수익률 계산 함수"""
        retry_count = 0
        while retry_count < 3:
            try:
                stock = yf.Ticker(ticker)
                hist = stock.history(start=self.start_date, end=self.end_date)
                
                if len(hist) >= 252:
                    weighted_return = self.calculate_weighted_return(hist)
                    time.sleep(uniform(0.5, 1))
                    return ticker, hist, weighted_return
                return ticker, pd.DataFrame(), None
                
            except Exception as e:
                retry_count += 1
                if "Too Many Requests" in str(e):
                    wait_time = 5 * retry_count
                    print(f"{ticker} 처리 중 요청 제한 발생. {wait_time}초 후 재시도...")
                    time.sleep(wait_time)
                else:
                    print(f"{ticker} 처리 중 오류 발생: {e}")
                    return ticker, pd.DataFrame(), None
        
        return ticker, pd.DataFrame(), None

    def analyze_stocks(self):
        """주식 분석 실행 - 병렬 처리 적용"""
        tickers = self.get_krx_tickers()
        returns_dict = {}
        stock_data = {}
        
        print(f"총 {len(tickers)}개 종목 분석 시작...")
        
        # 1단계: 병렬 처리로 전체 종목의 수익률 계산
        print("전체 종목 수익률 계산 중...")
        processed_count = 0
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_ticker = {executor.submit(self.get_stock_data_with_return, ticker): ticker for ticker in tickers}
            
            for future in concurrent.futures.as_completed(future_to_ticker):
                ticker = future_to_ticker[future]
                try:
                    ticker, hist, weighted_return = future.result()
                    if weighted_return is not None:
                        returns_dict[ticker] = weighted_return
                        stock_data[ticker] = hist
                    
                    processed_count += 1
                    if processed_count % 50 == 0:
                        print(f"{processed_count}/{len(tickers)} 종목 처리 완료...")
                        
                except Exception as e:
                    print(f"{ticker} 처리 중 오류 발생: {e}")
        
        # 2단계: RS 등급 계산
        print("RS 등급 계산 중...")
        rs_ratings = self.calculate_rs_rating(returns_dict)
        
        # 3단계: RS 70 이상 종목에 대해 기술적 조건 확인
        print("조건에 맞는 종목 선별 중...")
        selected_stocks = []
        
        for ticker, rs_value in rs_ratings.items():
            if rs_value >= 70:
                hist = stock_data[ticker]
                if self.check_technical_conditions(hist):
                    current_price = hist['Close'].iloc[0]
                    year_high = hist['High'].max()
                    
                    if current_price >= year_high * 0.75:
                        try:
                            stock_info = yf.Ticker(ticker).info
                            time.sleep(uniform(0.5, 1))
                            
                            selected_stocks.append({
                                'ticker': ticker,
                                'name': stock_info.get('longName', ''),
                                'current_price': current_price,
                                'change_percent': ((current_price / hist['Close'].iloc[1]) - 1) * 100,
                                'volume': hist['Volume'].iloc[0] * current_price,
                                'year_high': year_high,
                                'rs_rating': rs_value,
                                'market_cap': stock_info.get('marketCap', 0)
                            })
                        except Exception as e:
                            print(f"{ticker} 정보 가져오기 중 오류 발생: {e}")
                            continue

        print(f"총 {len(selected_stocks)}개 종목 선정 완료")
        return selected_stocks

    # 나머지 메서드들은 이전과 동일...

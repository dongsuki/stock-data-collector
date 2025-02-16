import requests
from bs4 import BeautifulSoup
import pandas as pd

def get_eps_trend_data(symbol):
    try:
        # Yahoo Finance Analysis 페이지 URL
        url = f'https://finance.yahoo.com/quote/{symbol}/analysis'
        
        # User-Agent 헤더 추가
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        print(f"Fetching EPS Trend data for {symbol}...")
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # EPS Trend 테이블 찾기
            tables = soup.find_all('table')
            for table in tables:
                # 테이블 헤더에서 "EPS Trend" 텍스트 찾기
                if table.find(text='EPS Trend'):
                    print("\nEPS Trend Data found:")
                    # 데이터 행 추출
                    rows = table.find_all('tr')
                    for row in rows:
                        # 각 셀의 데이터 추출
                        cells = row.find_all('td')
                        if cells:
                            print([cell.text.strip() for cell in cells])
                    return True
            
            print("EPS Trend table not found")
            return False
            
        else:
            print(f"Failed to fetch data. Status code: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"Error occurred: {e}")
        return False

def main():
    symbol = 'INOD'
    success = get_eps_trend_data(symbol)
    print(f"\nData collection for {symbol}: {'successful' if success else 'failed'}")

if __name__ == "__main__":
    main()

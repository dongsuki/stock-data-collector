import requests
from bs4 import BeautifulSoup
import pandas as pd

def get_eps_trend_data(symbol):
    try:
        url = f'https://finance.yahoo.com/quote/{symbol}/analysis'
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        print(f"Fetching EPS Trend data for {symbol}...")
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 모든 테이블 가져오기
            tables = soup.find_all('table')
            print(f"Found {len(tables)} tables")
            
            # 각 테이블의 내용 확인
            for i, table in enumerate(tables):
                print(f"\nTable {i + 1}:")
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if cells:
                        print([cell.text.strip() for cell in cells])
            
            return True
            
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

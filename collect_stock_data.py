import os
import requests
from datetime import datetime
import time
from airtable import Airtable

# API Keys and Constants
FMP_API_KEY = "EApxNJTRwcXOrhy2IUqSeKV0gyH8gans"
AIRTABLE_API_KEY = "patBy8FRWWiG6P99a.a0670e9dd25c84d028c9f708af81d5f1fb164c3adeb1cee067d100075db8b748"
AIRTABLE_BASE_ID = "appAh82iPV3cH6Xx5"
TABLE_NAME = "미국주식 데이터"

# Initialize Airtable
airtable = Airtable(AIRTABLE_BASE_ID, TABLE_NAME, AIRTABLE_API_KEY)

def get_gainers():
    """Get top gainers from FMP API"""
    url = f"https://financialmodelingprep.com/api/v3/stock_market/gainers?apikey={FMP_API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    return []

def filter_stocks(stocks):
    """Filter stocks based on criteria"""
    filtered = []
    for stock in stocks:
        if (stock['price'] >= 5 and  # Price >= $5
            stock['volume'] >= 1000000 and  # Volume >= 1M
            stock['changesPercentage'] >= 5 and  # Change >= 5%
            stock['marketCap'] >= 100000000):  # Market Cap >= $100M
            filtered.append(stock)
    return filtered

def map_exchange(exchange):
    """Map exchange codes to proper names"""
    exchange_map = {
        'XNAS': 'NASDAQ',
        'XNYS': 'NYSE',
        'XASE': 'AMEX'
    }
    return exchange_map.get(exchange, exchange)

def prepare_airtable_record(stock):
    """Prepare stock data for Airtable"""
    return {
        "fields": {
            "티커": stock['symbol'],
            "종목명": stock['name'],
            "현재가": stock['price'],
            "등락률": stock['changesPercentage'],
            "거래량": stock['volume'],
            "시가총액": stock['marketCap'],
            "거래소 정보": map_exchange(stock['exchange']),
            "업데이트 시간": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "분류": "전일대비등락률상위"
        }
    }

def main():
    # Get gainers data
    print("Fetching gainers data...")
    gainers = get_gainers()
    
    # Filter stocks based on criteria
    print("Filtering stocks...")
    filtered_stocks = filter_stocks(gainers)
    
    # Upload to Airtable
    print(f"Uploading {len(filtered_stocks)} stocks to Airtable...")
    for stock in filtered_stocks:
        record = prepare_airtable_record(stock)
        try:
            airtable.insert(record['fields'])
            time.sleep(0.2)  # Rate limiting
        except Exception as e:
            print(f"Error uploading {stock['symbol']}: {str(e)}")

    print("Process completed!")

if __name__ == "__main__":
    main()

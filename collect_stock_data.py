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
        data = response.json()
        print(f"Retrieved {len(data)} gainers")
        # Print first record for debugging
        if data:
            print("Sample record:", data[0])
        return data
    print(f"Error getting gainers: {response.status_code}")
    return []

def filter_stocks(stocks):
    """Filter stocks based on criteria"""
    filtered = []
    print(f"Filtering {len(stocks)} stocks...")
    for stock in stocks:
        try:
            if (float(stock.get('price', 0)) >= 5 and  # Price >= $5
                float(stock.get('avgVolume', 0)) >= 1000000 and  # Volume >= 1M
                float(stock.get('changesPercentage', 0)) >= 5 and  # Change >= 5%
                float(stock.get('marketCap', 0)) >= 100000000):  # Market Cap >= $100M
                filtered.append(stock)
        except (ValueError, TypeError) as e:
            print(f"Error processing stock {stock.get('symbol', 'Unknown')}: {str(e)}")
            print(f"Stock data: {stock}")
            continue
    
    print(f"Found {len(filtered)} stocks matching criteria")
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
    try:
        return {
            "fields": {
                "티커": stock.get('symbol', ''),
                "종목명": stock.get('name', ''),
                "현재가": float(stock.get('price', 0)),
                "등락률": float(stock.get('changesPercentage', 0)),
                "거래량": float(stock.get('avgVolume', 0)),
                "시가총액": float(stock.get('marketCap', 0)),
                "거래소 정보": map_exchange(stock.get('exchange', '')),
                "업데이트 시간": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "분류": "전일대비등락률상위"
            }
        }
    except (ValueError, TypeError) as e:
        print(f"Error preparing record for {stock.get('symbol', 'Unknown')}: {str(e)}")
        return None

def main():
    print("Starting stock data collection...")
    
    # Get gainers data
    print("Fetching gainers data...")
    gainers = get_gainers()
    
    if not gainers:
        print("No gainers data retrieved. Exiting.")
        return
    
    # Filter stocks based on criteria
    print("Filtering stocks...")
    filtered_stocks = filter_stocks(gainers)
    
    if not filtered_stocks:
        print("No stocks matched the criteria. Exiting.")
        return
    
    # Upload to Airtable
    print(f"Uploading {len(filtered_stocks)} stocks to Airtable...")
    success_count = 0
    
    for stock in filtered_stocks:
        record = prepare_airtable_record(stock)
        if record:
            try:
                airtable.insert(record['fields'])
                success_count += 1
                time.sleep(0.2)  # Rate limiting
            except Exception as e:
                print(f"Error uploading {stock.get('symbol', 'Unknown')}: {str(e)}")
    
    print(f"Process completed! Successfully uploaded {success_count} records.")

if __name__ == "__main__":
    main()

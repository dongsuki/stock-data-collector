import os
import requests
from datetime import datetime, timedelta
from airtable import Airtable
import time
import pytz

# API Configuration
POLYGON_API_KEY = "lsstdMdFXY50qjPNMQrXFp4vAGj0bNd5"
AIRTABLE_API_KEY = "patBy8FRWWiG6P99a.a0670e9dd25c84d028c9f708af81d5f1fb164c3adeb1cee067d100075db8b748"
AIRTABLE_BASE_ID = "appAh82iPV3cH6Xx5"
AIRTABLE_TABLE_NAME = "Stocks"

# Initialize Airtable
airtable = Airtable(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME, AIRTABLE_API_KEY)

def get_polygon_headers():
    return {
        "Authorization": f"Bearer {POLYGON_API_KEY}"
    }

def get_all_tickers():
    """Get all stock tickers with basic filtering"""
    url = "https://api.polygon.io/v3/reference/tickers"
    params = {
        "active": True,
        "market": "stocks",
        "limit": 1000
    }
    
    all_tickers = []
    while True:
        response = requests.get(url, headers=get_polygon_headers(), params=params)
        data = response.json()
        
        if response.status_code != 200:
            print(f"Error fetching tickers: {data}")
            break
            
        all_tickers.extend(data["results"])
        
        if "next_url" not in data or not data["next_url"]:
            break
            
        url = data["next_url"]
        
    return all_tickers

def get_ticker_details(ticker):
    """Get detailed information for a specific ticker"""
    url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}"
    response = requests.get(url, headers=get_polygon_headers())
    return response.json()

def filter_gainers_losers(min_price=5, min_volume=1000000, min_change_percent=5, min_market_cap=100000000):
    """Get top gainers and losers with filtering"""
    url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/gainers"
    response = requests.get(url, headers=get_polygon_headers())
    data = response.json()
    
    filtered_stocks = []
    for ticker in data.get("tickers", []):
        if (ticker.get("day", {}).get("c", 0) >= min_price and
            ticker.get("day", {}).get("v", 0) >= min_volume and
            abs(ticker.get("todaysChangePerc", 0)) >= min_change_percent and
            ticker.get("market_cap", 0) >= min_market_cap):
            filtered_stocks.append(ticker)
    
    return filtered_stocks

def get_top_volume(limit=20, min_price=5, min_market_cap=100000000):
    """Get top volume stocks with filtering"""
    url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
    response = requests.get(url, headers=get_polygon_headers())
    data = response.json()
    
    filtered_stocks = []
    for ticker in data.get("tickers", []):
        if (ticker.get("day", {}).get("c", 0) >= min_price and
            ticker.get("market_cap", 0) >= min_market_cap):
            filtered_stocks.append(ticker)
    
    # Sort by volume and get top N
    filtered_stocks.sort(key=lambda x: x.get("day", {}).get("v", 0), reverse=True)
    return filtered_stocks[:limit]

def get_top_market_cap(limit=20, min_price=5, min_volume=1000000):
    """Get top market cap stocks with filtering"""
    url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
    response = requests.get(url, headers=get_polygon_headers())
    data = response.json()
    
    filtered_stocks = []
    for ticker in data.get("tickers", []):
        if (ticker.get("day", {}).get("c", 0) >= min_price and
            ticker.get("day", {}).get("v", 0) >= min_volume):
            filtered_stocks.append(ticker)
    
    # Sort by market cap and get top N
    filtered_stocks.sort(key=lambda x: x.get("market_cap", 0), reverse=True)
    return filtered_stocks[:limit]

def get_52_week_highs(limit=20, min_price=5, min_volume=1000000, min_market_cap=100000000):
    """Get stocks hitting 52-week highs"""
    url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
    response = requests.get(url, headers=get_polygon_headers())
    data = response.json()
    
    filtered_stocks = []
    for ticker in data.get("tickers", []):
        current_price = ticker.get("day", {}).get("c", 0)
        if (current_price >= min_price and
            ticker.get("day", {}).get("v", 0) >= min_volume and
            ticker.get("market_cap", 0) >= min_market_cap):
            # Add logic to check if it's a 52-week high
            filtered_stocks.append(ticker)
    
    return filtered_stocks[:limit]

def add_to_airtable(stocks, category):
    """Add stocks to Airtable"""
    est = pytz.timezone('US/Eastern')
    current_time = datetime.now(est).strftime("%Y-%m-%d %H:%M:%S %Z")
    
    for stock in stocks:
        record = {
            "티커": stock.get("ticker"),
            "종목명": stock.get("name", ""),
            "현재가": stock.get("day", {}).get("c", 0),
            "등락률": stock.get("todaysChangePerc", 0),
            "거래량": stock.get("day", {}).get("v", 0),
            "시가총액": stock.get("market_cap", 0),
            "거래소 정보": stock.get("primary_exchange", "").replace("XNAS", "NASDAQ").replace("XNYS", "NYSE").replace("XASE", "AMEX"),
            "업데이트 시간": current_time,
            "분류": category
        }
        
        try:
            airtable.insert(record)
            time.sleep(0.2)  # Rate limiting
        except Exception as e:
            print(f"Error adding record to Airtable: {e}")

def main():
    # Get and process gainers/losers
    gainers_losers = filter_gainers_losers()
    add_to_airtable(gainers_losers, "전일대비등락률상위")
    
    # Get and process top volume
    top_volume = get_top_volume()
    add_to_airtable(top_volume, "거래대금상위")
    
    # Get and process top market cap
    top_market_cap = get_top_market_cap()
    add_to_airtable(top_market_cap, "시가총액상위")
    
    # Get and process 52-week highs
    week_highs = get_52_week_highs()
    add_to_airtable(week_highs, "52주신고가")

if __name__ == "__main__":
    main()

import os
import requests
from datetime import datetime
from airtable import Airtable

# API 키 및 Airtable 설정
POLYGON_API_KEY = "lsstdMdFXY50qjPNMQrXFp4vAGj0bNd5"
AIRTABLE_API_KEY = "patBy8FRWWiG6P99a.a0670e9dd25c84d028c9f708af81d5f1fb164c3adeb1cee067d100075db8b748"
AIRTABLE_BASE_ID = "appAh82iPV3cH6Xx5"
AIRTABLE_TABLE_NAME = "StockData"

# 거래소 정보 변환
EXCHANGE_MAP = {"XNAS": "NASDAQ", "XNYS": "NYSE", "XASE": "AMEX"}

# Polygon.io API 엔드포인트
POLYGON_ENDPOINT = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks"

def fetch_polygon_data():
    """Polygon.io API에서 데이터 가져오기"""
    response = requests.get(f"{POLYGON_ENDPOINT}/tickers", params={"apiKey": POLYGON_API_KEY})
    response.raise_for_status()
    return response.json().get("tickers", [])

def filter_data(data, conditions):
    """데이터를 주어진 조건으로 필터링"""
    filtered = []
    for item in data:
        try:
            current_price = item["lastTrade"]["p"]
            volume = item["day"]["v"]
            change_percent = item["todaysChangePerc"]
            market_cap = item.get("market_cap", 0)
            exchange = EXCHANGE_MAP.get(item.get("primary_exchange"), "UNKNOWN")

            if (
                current_price >= conditions["min_price"] and
                volume >= conditions["min_volume"] and
                change_percent >= conditions["min_change_percent"] and
                market_cap >= conditions["min_market_cap"]
            ):
                filtered.append({
                    "ticker": item["ticker"],
                    "name": item.get("name", "N/A"),
                    "current_price": current_price,
                    "change_percent": change_percent,
                    "volume": volume,
                    "market_cap": market_cap,
                    "exchange": exchange,
                    "updated_at": datetime.now().isoformat()
                })
        except KeyError:
            continue
    return filtered

def save_to_airtable(data, category):
    """Airtable에 데이터 저장"""
    airtable = Airtable(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME, api_key=AIRTABLE_API_KEY)
    for record in data:
        record["category"] = category
        airtable.insert(record)

def main():
    """메인 실행 함수"""
    data = fetch_polygon_data()

    # 각 카테고리에 대한 조건 정의
    categories = {
        "전일대비등락률상위": {
            "min_price": 5,
            "min_volume": 1_000_000,
            "min_change_percent": 5,
            "min_market_cap": 100_000_000,
        },
        "거래대금상위": {
            "min_price": 5,
            "min_volume": 0,  # 제한 없음
            "min_change_percent": 0,  # 제한 없음
            "min_market_cap": 100_000_000,
        },
        "시가총액상위": {
            "min_price": 5,
            "min_volume": 1_000_000,
            "min_change_percent": 0,  # 제한 없음
            "min_market_cap": 0,  # 제한 없음
        },
        "52주 신고가": {
            "min_price": 5,
            "min_volume": 1_000_000,
            "min_change_percent": 0,  # 제한 없음
            "min_market_cap": 100_000_000,
        },
    }

    for category, conditions in categories.items():
        filtered_data = filter_data(data, conditions)
        save_to_airtable(filtered_data, category)
        print(f"Category '{category}' - {len(filtered_data)} records saved.")

if __name__ == "__main__":
    main()

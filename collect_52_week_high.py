def safe_float(value, default=0.0):
    """안전하게 float로 변환"""
    try:
        if value is None:
            return default
        return float(value)
    except (ValueError, TypeError):
        return default

def is_valid_us_stock(stock):
    """실제 거래 가능한 미국 주식인지 확인"""
    symbol = stock.get('symbol', '')
    exchange = stock.get('exchange', '')
    type = stock.get('type', '').lower()
    name = stock.get('name', '').lower()
    
    # ETF 제외
    if 'etf' in type:
        return False
        
    # 미국 거래소만 포함
    if exchange not in {'NYSE', 'NASDAQ'}:
        return False
        
    # 워런트, ADR, 우선주 등 제외
    invalid_keywords = [
        'warrant', 'warrants', 'adr', 'preferred', 
        'acquisition', 'right', 'rights',
        'merger', 'spac', 'trust', 'unit', 'units'
    ]
    if any(keyword in name.lower() for keyword in invalid_keywords):
        return False
        
    # 특수 심볼 포함된 것 제외
    if any(x in symbol for x in ['-', '^', '.', '$', '=', ' ']):
        return False
        
    # 심볼이 5자 이상인 경우 대부분 특수 증권
    if len(symbol) > 4:
        return False
    
    # 가격이 고정되어 있는 경우 (인수합병 등) 제외
    price = safe_float(stock.get('price'))
    dayHigh = safe_float(stock.get('dayHigh'))
    dayLow = safe_float(stock.get('dayLow'))
    if price > 0 and dayHigh == dayLow:  # 당일 고가/저가가 동일하면 거래정지 의심
        return False
    
    return True

def calculate_change_percent(stock):
    """현재가 기준 등락률 계산"""
    price = safe_float(stock.get('price'))
    previousClose = safe_float(stock.get('previousClose'))
    
    if previousClose > 0:
        return ((price - previousClose) / previousClose) * 100
    return 0.0

def get_quotes():
    """미국 주식 데이터 가져오기"""
    print("데이터 수집 시작...")
    
    # NASDAQ 데이터
    url = f"https://financialmodelingprep.com/api/v3/quotes/nasdaq?apikey={FMP_API_KEY}"
    try:
        response = requests.get(url, timeout=30)
        nasdaq_stocks = response.json() if response.status_code == 200 else []
        print(f"NASDAQ 종목 수집: {len(nasdaq_stocks)}개")
        
        # NYSE 데이터
        url = f"https://financialmodelingprep.com/api/v3/quotes/nyse?apikey={FMP_API_KEY}"
        response = requests.get(url, timeout=30)
        nyse_stocks = response.json() if response.status_code == 200 else []
        print(f"NYSE 종목 수집: {len(nyse_stocks)}개")
        
        all_stocks = nasdaq_stocks + nyse_stocks
        print(f"총 수집 종목 수: {len(all_stocks)}개")
        
        if all_stocks:
            print("\n첫 번째 종목 데이터 샘플:")
            print(all_stocks[0])
        
        return all_stocks
        
    except Exception as e:
        print(f"데이터 수집 중 에러: {str(e)}")
        return []

def filter_stocks(stocks):
    """주식 필터링"""
    print("\n필터링 시작...")
    print(f"필터링 전 종목 수: {len(stocks)}개")
    filtered = []
    invalid_count = 0
    
    for stock in stocks:
        try:
            if not is_valid_us_stock(stock):
                invalid_count += 1
                continue
                
            price = safe_float(stock.get('price'))
            volume = safe_float(stock.get('volume'))
            yearHigh = safe_float(stock.get('yearHigh'))
            marketCap = safe_float(stock.get('marketCap'))
            
            # 데이터 유효성 검사
            if price <= 0 or yearHigh <= 0:
                invalid_count += 1
                continue
                
            # 52주 고가 대비 현재가 비율
            price_to_high_ratio = (price / yearHigh) * 100
            
            # 등락률 계산
            change_percent = calculate_change_percent(stock)
            
            if (price >= 10 and 
                volume >= 1000000 and 
                marketCap >= 500000000 and 
                price_to_high_ratio >= 95):
                
                filtered.append({
                    'symbol': stock.get('symbol'),
                    'price': price,
                    'volume': volume,
                    'yearHigh': yearHigh,
                    'marketCap': marketCap,
                    'name': stock.get('name', ''),
                    'exchange': stock.get('exchange', ''),
                    'price_to_high_ratio': price_to_high_ratio,
                    'change_percent': change_percent,
                    'dayHigh': safe_float(stock.get('dayHigh')),
                    'dayLow': safe_float(stock.get('dayLow'))
                })
                
                print(f"\n조건 만족: {stock.get('symbol')}")
                print(f"종목명: {stock.get('name')}")
                print(f"가격: ${price:,.2f}")
                print(f"거래량: {volume:,.0f}")
                print(f"52주 고가: ${yearHigh:,.2f}")
                print(f"시가총액: ${marketCap:,.2f}")
                print(f"고가대비: {price_to_high_ratio:.1f}%")
                print(f"등락률: {change_percent:.1f}%")
                print(f"당일고가: ${safe_float(stock.get('dayHigh')):,.2f}")
                print(f"당일저가: ${safe_float(stock.get('dayLow')):,.2f}")
                
        except Exception as e:
            invalid_count += 1
            continue
    
    print(f"\n유효하지 않은 데이터: {invalid_count}개")
    print(f"조건 만족 종목: {len(filtered)}개")
    
    return sorted(filtered, key=lambda x: x['price_to_high_ratio'], reverse=True)

def update_airtable(stocks):
    """Airtable 업데이트"""
    print("\nAirtable 업데이트 시작...")
    airtable = Airtable(AIRTABLE_BASE_ID, TABLE_NAME, AIRTABLE_API_KEY)
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    for i, stock in enumerate(stocks, 1):
        try:
            # 티커로 기존 레코드 검색
            existing_records = airtable.search('티커', stock['symbol'])
            
            record = {
                '티커': stock['symbol'],
                '종목명': stock['name'],
                '현재가': stock['price'],
                '등락률': stock['change_percent'],
                '거래량': stock['volume'],
                '시가총액': stock['marketCap'],
                '업데이트 시간': current_date,
                '분류': "52주_신고가_근접",
                '거래소 정보': stock['exchange'],
                '신고가 비율(%)': stock['price_to_high_ratio']
            }
            
            if existing_records:
                # 기존 레코드가 있으면 업데이트
                record_id = existing_records[0]['id']
                airtable.update(record_id, record)
                print(f"[{i}/{len(stocks)}] {stock['symbol']} 업데이트 완료")
            else:
                # 기존 레코드가 없으면 새로 추가
                airtable.insert(record)
                print(f"[{i}/{len(stocks)}] {stock['symbol']} 신규 추가")
                
            time.sleep(0.2)
        except Exception as e:
            print(f"Airtable 업데이트 실패 ({stock['symbol']}): {str(e)}")

def main():
    print("프로그램 시작...")
    print("필터링 조건:")
    print("- 현재가 >= $10")
    print("- 거래량 >= 1,000,000주")
    print("- 시가총액 >= $500,000,000")
    print("- 현재가가 52주 고가의 95% 이상")
    print("- 실제 거래 가능한 미국 주식만")
    
    start_time = time.time()
    
    stocks = get_quotes()
    if not stocks:
        print("데이터 수집 실패")
        return
    
    filtered_stocks = filter_stocks(stocks)
    
    if filtered_stocks:
        update_airtable(filtered_stocks)
    
    end_time = time.time()
    print(f"\n처리 완료! (소요시간: {end_time - start_time:.2f}초)")

if __name__ == "__main__":
    main()

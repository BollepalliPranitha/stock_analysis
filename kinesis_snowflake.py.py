import json
import base64
from datetime import datetime
from statistics import mean
import time


# Helper function for moving average calculation
def calculate_moving_average(prices, window_size):
    if len(prices) < window_size:
        return None
    return mean(prices[-window_size:])

symbol_prices = {}

def lambda_handler(event, context):
    transformed_records = []

    for record in event['records']:
        try:
            # Decode base64 and parse JSON
            payload_json = base64.b64decode(record['data']).decode('utf-8')
            payload = json.loads(payload_json)

            # Extract CSV string from 'stock_data'
            csv_line = payload['stock_data']
            parts = csv_line.strip().split(',')

            # Map CSV to fields
            date = parts[0]
            symbol = parts[1]
            open_price = float(parts[2])
            high = float(parts[3])
            low = float(parts[4])
            close = float(parts[5])
            adjusted_close = float(parts[6])
            volume = float(parts[7])

            # Placeholder SP500 index, update if available
            sp500_index = 0.025

            # Date fields
            date_obj = datetime.strptime(date, "%Y-%m-%d")
            day_of_week = date_obj.isoweekday()
            week_of_year = date_obj.isocalendar()[1]
            month = date_obj.month
            quarter = (month - 1) // 3 + 1
            year = date_obj.year

            # Calculations
            daily_return = (adjusted_close - open_price) / open_price
            daily_volatility = high - low
            volume_spike = False  # Placeholder logic
            excess_return = daily_return - sp500_index

            if symbol not in symbol_prices:
                symbol_prices[symbol] = []
            symbol_prices[symbol].append(adjusted_close)

            ma5 = calculate_moving_average(symbol_prices[symbol], 5)
            ma20 = calculate_moving_average(symbol_prices[symbol], 20)
            ma50 = calculate_moving_average(symbol_prices[symbol], 50)
            stock_perf_key = int(time.time() * 1000)

            transformed_payload = {
            "StockPerfKey": stock_perf_key, 
            "Date": date, 
            "StockSymbol": symbol,
            "AdjustedClose": adjusted_close,
            "Close": close,
            "High": high,
            "Low": low,
            "Open": open_price,
            "Volume": int(volume),
            "DailyReturn": daily_return,
            "DailyVolatility": daily_volatility,
            "VolumeSpike": volume_spike,
            "ExcessReturn": excess_return,
            "MA5": ma5,
            "MA20": ma20,
            "MA50": ma50,
    # You can either add placeholders for missing columns, e.g. None
            "CurrentPrice": None,
            "MarketCap": None,
            "EBITDA": None,
            "RevenueGrowth": None,
            "FullTimeEmployees": None,
            "Weight": None
}

            encoded = base64.b64encode(json.dumps(transformed_payload).encode('utf-8')).decode('utf-8')

            transformed_records.append({
                'recordId': record['recordId'],
                'result': 'Ok',
                'data': encoded
            })

        except Exception as e:
            print(f"Error processing record {record['recordId']}: {str(e)}")
            transformed_records.append({
                'recordId': record['recordId'],
                'result': 'ProcessingFailed',
                'data': record['data']
            })

    return {'records': transformed_records}
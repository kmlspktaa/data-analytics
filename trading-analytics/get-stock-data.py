import yfinance as yf
import json  # Ensure the built-in json module is imported

ticker_symbol = "AAPL"
stock = yf.Ticker(ticker_symbol)

historical_data = stock.history(period="1y").reset_index().to_dict(orient="records")

if historical_data:
    with open("apple_stock_data.json", "w") as json_file:
        for record in historical_data:
            json_file.write(json.dumps(record, default=str) + '\n')
    print(f"{ticker_symbol} stock data has been saved to apple_stock_data.json!")
else:
    print(f"No data found for ticker {ticker_symbol}.")

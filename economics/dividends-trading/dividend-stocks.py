import yfinance as yf
import pandas as pd

# Define the symbols and year
symbols = ["AAPL", "O", "JNJ", "CNQ", "PEP", "T", "PG", "LTC", "MCD", "JPM", "CAT", "IBM", "CAH", "GD", "RY", "BMO", "BATS", "BNS", "CB", "LOW", "SYY", "ITW", "WMT", "KMB", "RKT", "MAIN", "ADP", "SBUX", "ECL", "CVX", "SEMB", "PSEC", "AGNC", "STAG", "AFLPPG", "MDT", "NUE", "EMR", "TROW", "BEN", "STHS", "SHW", "SSHY", "VUSC", "GWW", "ROP", "ADC"]
year = 2022

# Create an empty list to store the dataframes for each stock
dfs = []

# Iterate over the symbols
for symbol in symbols:
    try:
        # Get the dividends data for the specified year
        stock = yf.Ticker(symbol)
        dividends = stock.dividends
        dividends_2022 = dividends.loc[str(year)]

        if dividends_2022.empty:
            print(f"No dividend data available for {symbol} in {year}.")
            continue

        # Get the market capitalization data
        try:
            market_cap = float(stock.info["marketCap"])
        except KeyError:
            market_cap = None

        # Create a dataframe for the current stock
        data = {
            "Symbol": symbol,
            "Year": year,
            "Dividend Date": dividends_2022.index.strftime('%m/%d/%Y'),
            "Market Capitalization": market_cap,
            "Count of total dividends paid for that year": len(dividends_2022),
            "How much was paid": dividends_2022.tolist()
        }
        df = pd.DataFrame(data)

        # Append the dataframe to the list
        dfs.append(df)

    except Exception as e:
        print(f"Got error from Yahoo API for ticker {symbol}, Error: {str(e)}")
        print(f"Skipping symbol {symbol} due to data unavailability.")

# Concatenate the dataframes for all stocks
result_df = pd.concat(dfs)

# Convert columns to float type
result_df["Market Capitalization"] = result_df["Market Capitalization"].astype(float)
result_df["Count of total dividends paid for that year"] = result_df["Count of total dividends paid for that year"].astype(float)

# Save the dataframe to a CSV file
result_df.to_csv("dividend_data_2022.csv", index=False)

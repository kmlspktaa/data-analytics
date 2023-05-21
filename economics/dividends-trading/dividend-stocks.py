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
        # Get the dividends data for 2022
        stock = yf.Ticker(symbol)
        dividends = stock.dividends

        if isinstance(dividends, list):
            print(f"Skipping symbol {symbol} due to data unavailability.")
            continue

        dividends_2022 = dividends.loc[str(year)]

        dividend_dates = dividends_2022.index.strftime('%m/%d/%Y').tolist()
        dividend_count = len(dividend_dates)
        dividend_payment = dividends_2022.sum()

        # Get the market capitalization data
        try:
            market_cap = "{:,.0f}".format(stock.info["marketCap"])
        except KeyError:
            market_cap = None

        # Create a dataframe for the current stock
        data = {
            "Symbol": [symbol] * dividend_count,
            "Year": [year] * dividend_count,
            "Dividend Date": dividend_dates,
            "Market Capitalization": [market_cap] * dividend_count,
            "Count of total dividends paid for that year": [dividend_count] * dividend_count,
            "How much was paid": [f"{dividend_payment:.2f} Dividend"] * dividend_count
        }
        df = pd.DataFrame(data)

        # Append the dataframe to the list
        dfs.append(df)

    except Exception as e:
        print(f"Got error from Yahoo API for ticker {symbol}, Error: {str(e)}")
        print(f"Skipping symbol {symbol} due to data unavailability.")

# Concatenate the dataframes for all stocks
result_df = pd.concat(dfs)

# Convert market capitalization to numeric
result_df["Market Capitalization"] = pd.to_numeric(result_df["Market Capitalization"].str.replace(",", ""), errors="coerce")

# Sort the DataFrame by market capitalization in descending order
result_df = result_df.sort_values(by="Market Capitalization", ascending=False)

# Reset the index
result_df = result_df.reset_index(drop=True)

# Add a column for placing/ranking based on market capitalization
result_df["Placing"] = result_df.index + 1

# Save the dataframe to an XLSX file
result_df.to_excel("market_cap_data.xlsx", index=False)

import pandas as pd
import yfinance as yf

# Read the symbols from a CSV file
symbols_df = pd.read_csv("symbols.csv")
symbols = symbols_df["Symbol"].tolist()

# Specify the years
years = [2021, 2022]

# Create an empty list to store the dataframes for each stock
dfs = []

# Iterate over the symbols
for symbol in symbols:
    try:
        # Get the dividends data for the specified years
        stock = yf.Ticker(symbol)
        dividends = stock.dividends

        for year in years:
            dividends_year = dividends.loc[str(year)]

            if dividends_year.empty:
                print(f"No dividend data available for {symbol} in {year}.")
                continue

            # Get the market capitalization data
            try:
                market_cap = float(stock.info["marketCap"])
            except KeyError:
                market_cap = None

            # Create a dataframe for the current stock and year
            data = {
                "Symbol": symbol,
                "Year": year,
                "Dividend Date": dividends_year.index.strftime('%m/%d/%Y'),
                "Market Capitalization": market_cap,
                "Count of total dividends paid for that year": len(dividends_year),
                "How much was paid": dividends_year.tolist()
            }
            df = pd.DataFrame(data)

            # Append the dataframe to the list
            dfs.append(df)

    except Exception as e:
        print(f"Got error from Yahoo API for ticker {symbol}, Error: {str(e)}")
        print(f"Skipping symbol {symbol} due to data unavailability.")

# Concatenate the dataframes for all stocks and years
result_df = pd.concat(dfs)

# Convert columns to float type
result_df["Market Capitalization"] = result_df["Market Capitalization"].astype(float)
result_df["Count of total dividends paid for that year"] = result_df["Count of total dividends paid for that year"].astype(float)

# Save the dataframe to a CSV file
result_df.to_csv("temp_dividend_data_2022_2023.csv", index=False)

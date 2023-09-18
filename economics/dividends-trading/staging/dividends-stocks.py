import pandas as pd
import yfinance as yf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, format_number


class DividendAnalyzer:
    def __init__(self, symbols_file, output_file):
        self.symbols_file = symbols_file
        self.output_file = output_file
        self.symbols_df = pd.read_csv(symbols_file)
        self.symbols = self.symbols_df["Symbol"].tolist()
        self.years = [2021, 2022]
        self.dfs = []
        self.spark = SparkSession.builder.getOrCreate()
        self.df = None

    def fetch_dividend_data(self):
        for symbol in self.symbols:
            try:
                stock = yf.Ticker(symbol)
                dividends = stock.dividends

                for year in self.years:
                    dividends_year = dividends.loc[str(year)]

                    if dividends_year.empty:
                        print(f"No dividend data available for {symbol} in {year}.")
                        continue

                    try:
                        market_cap = float(stock.info["marketCap"])
                    except KeyError:
                        market_cap = None

                    stock_price = stock.history(start=f"{year}-01-01", end=f"{year}-12-31")
                    price_at_beginning = stock_price.iloc[0]['Close']
                    price_at_end = stock_price.iloc[-1]['Close']

                    data = {
                        "Symbol": symbol,
                        "Year": year,
                        "Dividend Date": dividends_year.index.strftime('%m/%d/%Y'),
                        "Market Capitalization": market_cap,
                        "Count of total dividends paid for that year": len(dividends_year),
                        "How much was paid": dividends_year.tolist(),
                        "Price at Beginning of Year": price_at_beginning,
                        "Price at End of Year": price_at_end
                    }
                    df = pd.DataFrame(data)
                    self.dfs.append(df)

            except Exception as e:
                print(f"Got error from Yahoo API for ticker {symbol}, Error: {str(e)}")
                print(f"Skipping symbol {symbol} due to data unavailability.")

    def save_dividend_data(self):
        result_df = pd.concat(self.dfs)
        result_df["Market Capitalization"] = result_df["Market Capitalization"].astype(float)
        result_df["Count of total dividends paid for that year"] = result_df[
            "Count of total dividends paid for that year"].astype(float)
        result_df.to_csv(self.output_file, index=False)

    def process_dividend_data(self):
        self.df = self.spark.read.csv(self.output_file, header=True)
        self.df = self.df.withColumn("How much was paid", col("How much was paid").cast("double"))
        self.df_filtered = self.df.filter((col("Year") == 2023) | (col("Year") == 2022) | (col("Year") == 2021))
        self.total_dividends = self.df_filtered.groupBy("Symbol", "Year").sum("How much was paid")
        self.total_dividends = self.total_dividends.withColumn("total_yearly_dividends",
                                                               round(col("sum(How much was paid)"), 2))
        self.total_dividends = self.total_dividends.join(
            self.df.select("Symbol", "Market Capitalization", "Price at Beginning of Year", "Price at End of Year",
                           "Year"),
            ["Symbol", "Year"]
        )
        self.total_dividends = self.total_dividends.withColumnRenamed("Market Capitalization", "Market_Cap")
        self.total_dividends = self.total_dividends.withColumn("Market_Cap", col("Market_Cap").cast("double"))
        self.total_dividends = self.total_dividends.select(
            "Symbol", "total_yearly_dividends",
            format_number(col("Market_Cap"), 0).alias("Market_Cap"),
            "Price at Beginning of Year", "Price at End of Year", "Year"
        )
        self.total_dividends = self.total_dividends.dropDuplicates(["Symbol", "Year"])
        self.total_dividends = self.total_dividends.orderBy(col("Year").asc(), col("total_yearly_dividends").desc())
        pandas_df = self.total_dividends.toPandas()
        pandas_df.to_excel(self.output_file.replace(".csv", ".xlsx"), index=False)

    def analyze_dividends(self):
        self.fetch_dividend_data()
        self.save_dividend_data()
        self.process_dividend_data()


dividend_analyzer = DividendAnalyzer("symbols.csv", "temp_dividend_data_2022_2023.csv")
dividend_analyzer.analyze_dividends()

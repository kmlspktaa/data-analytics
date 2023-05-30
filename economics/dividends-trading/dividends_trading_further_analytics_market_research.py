from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, format_number, lit
import pandas as pd

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Read the CSV file into a DataFrame
df = spark.read.csv("temp_dividend_data_2022_2023.csv", header=True)

# Convert "How much was paid" column to a numeric data type
df = df.withColumn("How much was paid", col("How much was paid").cast("double"))

# Select rows for the desired years (2023, 2022, 2021)
df_filtered = df.filter((col("Year") == 2023) | (col("Year") == 2022) | (col("Year") == 2021))

# Group by symbol and calculate the sum of dividends for each year
total_dividends = df_filtered.groupBy("Symbol", "Year").sum("How much was paid")

# Round the sum of dividends to double digits
total_dividends = total_dividends.withColumn("total_yearly_dividends", round(col("sum(How much was paid)"), 2))

# Join with the original DataFrame to include Market Capitalization and Price information
total_dividends = total_dividends.join(df.select("Symbol", "Market Capitalization", "Price at Beginning of Year", "Price at End of Year", "Year"), ["Symbol", "Year"])

# Rename "Market Capitalization" column to "Market_Cap"
total_dividends = total_dividends.withColumnRenamed("Market Capitalization", "Market_Cap")

# Convert "Market_Cap" column to a numeric data type
total_dividends = total_dividends.withColumn("Market_Cap", col("Market_Cap").cast("double"))

# Select Symbol, total_yearly_dividends, and format Market_Cap column
total_dividends = total_dividends.select("Symbol", "total_yearly_dividends", format_number(col("Market_Cap"), 0).alias("Market_Cap"), "Price at Beginning of Year", "Price at End of Year", "Year")

# Drop duplicate rows based on the "Symbol" and "Year" columns
total_dividends = total_dividends.dropDuplicates(["Symbol", "Year"])

# Order by year and highest total_yearly_dividends in descending order
total_dividends = total_dividends.orderBy(col("Year").asc(), col("total_yearly_dividends").desc())

# Show the total dividends for each symbol and year
# total_dividends.show()

# Convert PySpark DataFrame to Pandas DataFrame
pandas_df = total_dividends.toPandas()

# Save the Pandas DataFrame as an Excel file
pandas_df.to_excel("report_1.xlsx", index=False)

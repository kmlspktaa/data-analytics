from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Read the CSV file into a DataFrame
df = spark.read.csv("temp_dividend_data_2022_2023.csv", header=True)

# Convert "How much was paid" column to a numeric data type
df = df.withColumn("How much was paid", col("How much was paid").cast("double"))

# Group by symbol and calculate the sum of dividends
total_dividends = df.groupBy("Symbol").sum("How much was paid")

# Round the sum of dividends to double digits
total_dividends = total_dividends.withColumn("total_yearly_dividends", round(col("sum(How much was paid)"), 2))

# Select only the Symbol and total_yearly_dividends columns
total_dividends = total_dividends.select("Symbol", "total_yearly_dividends")

# Sort the total dividends in descending order of total_yearly_dividends
total_dividends = total_dividends.orderBy(col("total_yearly_dividends").desc())

# Show the total dividends for each symbol
total_dividends.show()
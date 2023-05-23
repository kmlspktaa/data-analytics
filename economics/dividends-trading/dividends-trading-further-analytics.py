from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Read the CSV file into a DataFrame
df = spark.read.csv("market_cap_data.csv", header=True)

# Convert "How much was paid" column to a numeric data type
df = df.withColumn("How much was paid", col("How much was paid").cast("double"))

# Group by symbol and calculate the sum of dividends
total_dividends = df.groupBy("Symbol").sum("How much was paid")

# Show the total dividends for each symbol
total_dividends.show()

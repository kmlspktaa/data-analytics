from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, format_number

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Read the CSV file into a DataFrame
df = spark.read.csv("dividend_data_2022.csv", header=True)

# Convert "How much was paid" column to a numeric data type
df = df.withColumn("How much was paid", col("How much was paid").cast("double"))

# Group by symbol and calculate the sum of dividends
total_dividends = df.groupBy("Symbol").sum("How much was paid")

# Round the sum of dividends to double digits
total_dividends = total_dividends.withColumn("total_yearly_dividends", round(col("sum(How much was paid)"), 2))

# Join with the original DataFrame to include Market Capitalization
total_dividends = total_dividends.join(df.select("Symbol", "Market Capitalization"), "Symbol")

# Rename "Market Capitalization" column to "Market_Cap"
total_dividends = total_dividends.withColumnRenamed("Market Capitalization", "Market_Cap")

# Convert "Market_Cap" column to a numeric data type
total_dividends = total_dividends.withColumn("Market_Cap", col("Market_Cap").cast("double"))

# Select Symbol, total_yearly_dividends, and format Market_Cap column
total_dividends = total_dividends.select("Symbol", "total_yearly_dividends", format_number(col("Market_Cap"), 0).alias("Market_Cap"))

# Drop duplicate rows based on the "Symbol" column
total_dividends = total_dividends.dropDuplicates()

# Order by highest total_yearly_dividends in descending order
total_dividends = total_dividends.orderBy(col("total_yearly_dividends").desc())

# Show the total dividends for each symbol
total_dividends.show()

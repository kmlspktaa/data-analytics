from pyspark.sql import SparkSession
from pyspark.sql.functions import udf

# Create Spark Session
spark = SparkSession.builder \
    .appName('UDF Example') \
    .getOrCreate()

# Create a Python function
def double(x):
    return 2 * x

# Convert the Python function to a PySpark UDF
double_udf = udf(double)

# Create a DataFrame
df = spark.createDataFrame([(1), (2), (3)], ["number"])

# Use the UDF to double all numbers in the DataFrame
df = df.withColumn("number_doubled", double_udf(df["number"]))

df.show()
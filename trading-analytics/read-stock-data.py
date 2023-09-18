from pyspark.sql import SparkSession, DataFrame

class JSONReader:
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.spark = self._initialize_spark()

    def _initialize_spark(self) -> SparkSession:
        """Initialize and return a Spark session."""
        spark = SparkSession.builder \
            .appName("JSONReaderApp") \
            .getOrCreate()
        return spark

    def get_dataframe(self) -> DataFrame:
        """Read the JSON file and return it as a DataFrame."""
        df = self.spark.read.json(self.filepath)
        df.cache()  # Cache the dataframe
        return df

if __name__ == "__main__":
    json_path = "./apple_stock_data.json"
    reader = JSONReader(json_path)
    df = reader.get_dataframe()
    df.show()
    if "_corrupt_record" in df.columns:
        print("Corrupt records found!")
        df.select("_corrupt_record").show()

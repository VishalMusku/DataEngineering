from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SimpleFilterTest") \
    .config("spark.network.timeout", "800s") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .getOrCreate()

print("Spark Session Initialized.")

# Create a simple DataFrame
data = [(1, "Alice", 20),
        (2, "Bob", 25),
        (3, "Cathy", 22),
        (4, "David", 30),
        (5, "Eva", 28)]
columns = ["ID", "Name", "Age"]

df = spark.createDataFrame(data, columns)

# Show the original DataFrame
print("Original DataFrame:")
df.show()

# Filter rows where Age is greater than 25
filtered_df = df.filter(col("Age") > 25)

# Show the filtered DataFrame
print("Filtered DataFrame (Age > 25):")
filtered_df.show()

# Stop the Spark session
spark.stop()
print("Spark Session Stopped.")

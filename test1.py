from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("PySpark Test Application") \
    .getOrCreate()

# Create a simple DataFrame
data = [("Alice", 30), ("Bob", 45), ("Charlie", 25), ("David", 35)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Show the initial DataFrame
print("Initial DataFrame:")
df.show()



# Perform a simple transformation: filter by Age > 30
filtered_df = df.filter(df.Age > 30)

# Show the result of the transformation
print("Filtered DataFrame (Age > 30):")
filtered_df.show()

print('Thanks!')




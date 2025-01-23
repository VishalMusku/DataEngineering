from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("BatchProcessingCheck") \
    .getOrCreate()

print("Spark Session Initialized.")

# Create a simple DataFrame
data = [(1, "Alice"), (2, "Bob"), (3, "Cathy")]
columns = ["ID", "Name"]

df = spark.createDataFrame(data, columns)

# Perform a transformation
transformed_df = df.withColumnRenamed("Name", "FullName")

# Action: Show the results
transformed_df.show()

# Optional Action: Write to a file to ensure cluster processes data
transformed_df.write.mode("overwrite").csv("./tmp/output_directory")

print("Processing Completed.")

# Stop the Spark session
spark.stop()

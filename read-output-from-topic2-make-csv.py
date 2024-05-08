from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.types import StructType

# Create a SparkSession
spark = SparkSession.builder.appName("KafkaToCSV3").getOrCreate()

# Define the schema for the DataFrame
schema = StructType().add("id", "string").add("date", "string").add("value", "string").add("label", "string").add("detection", "string")

# Read data from Kafka topic "vis" as a streaming DataFrame
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "vis3") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Convert the value column to a string
df = df.selectExpr("CAST(value AS STRING)")

# Split the value column into individual columns
df = df.select(
    expr("CAST(get_json_object(value, '$.id') AS STRING)").alias("id"),
    expr("CAST(get_json_object(value, '$.date') AS STRING)").alias("date"),
    expr("CAST(get_json_object(value, '$.value') AS STRING)").alias("value"),
    expr("CAST(get_json_object(value, '$.label') AS STRING)").alias("label"),
    expr("CAST(get_json_object(value, '$.detection') AS STRING)").alias("detection")
)


# Define the output directory for the CSV files
output_dir = "output7"

# Write the streaming DataFrame to the CSV sink
query = df.writeStream.format("csv") \
    .option("path", output_dir) \
    .option("checkpointLocation", "/checkpoints") \
    .start()

# Wait for the query to finish
query.awaitTermination()

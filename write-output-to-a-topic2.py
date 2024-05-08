import logging
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, count
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
from datetime import timedelta,datetime
import pandas as pd
from pyspark.sql.window import Window

# Set log level to ERROR for Py4j
logging.getLogger("py4j").setLevel(logging.ERROR)

spark = SparkSession.builder.appName("AbnormalDetection").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("id", StringType(), True),
    StructField("date", TimestampType(), True),
    StructField("value", DoubleType(), True),
    StructField("label", StringType(), True)
])

z_threshold = 1.5
d_threshold = 4

epsilon = 1e-8
# Define a function to process each batch
def process_batch(df, epoch_id):
    
     # Sort the DataFrame by 'date' to ensure consecutive values are considered
     df = df.orderBy("date")

     # Approach 1: Z-score anomaly detection
     mean_value = df.selectExpr("avg(value)").first()[0]
     stddev_value = df.selectExpr("stddev(value)").first()[0]

     z_score = (col("value") - mean_value) / stddev_value
z_detection = when(z_score > z_threshold, 1).otherwise(0)

    # Approach 2: Mean of distances anomaly detection
     df = df.withColumn("distance", F.abs(col("value") - F.lag("value").over(Window.orderBy("date"))))
     mean_distance = df.selectExpr("avg(distance) as mean_distance").first()["mean_distance"]
     threshold_distance = mean_distance * d_threshold
     distance_detection = when(col("distance") > threshold_distance, 1).otherwise(0)

    # Combine both detection results using logical OR
     combined_detection = (z_detection == 1) | (distance_detection == 1)
    # Convert boolean to integer (1 or 0)
     combined_detection = combined_detection.cast("int")
    # Add the combined detection column to the DataFrame
     df_with_combined_detection = df.withColumn("detection", combined_detection)

    # Select the desired columns for output
     output_df = df_with_combined_detection.select("id", "date", "value", "label", "detection")

    # Write the output to Kafka topic
     output_df.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "vis3") \
        .save()


     output_dir = "output2"  # Specify the output directory path
      output_file = f"{output_dir}/output_batch_{epoch_id}.csv"
     df_with_combined_detection.write.csv(output_file, mode="overwrite", header=True)

    

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "final-project") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")


df.writeStream.trigger(processingTime='5 seconds').foreachBatch(process_batch).outputMode("update").option("checkpointLocation", "/checkpoints").start().awaitTermination()


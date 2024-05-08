import logging
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
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

# Initialize metrics variables
total_events = 0
total_anomalies = 0
tp_total = 0
fp_total = 0
fn_total = 0

# Define a function to process each batch
def process_batch(df, epoch_id):
    global total_events, total_anomalies, tp_total, fp_total, fn_total

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

    # Update metrics
    total_events += df_with_combined_detection.count()
    total_anomalies += df_with_combined_detection.filter(col("detection") == 1).count()
    tp_total += df_with_combined_detection.filter((col("label") == 1) & (col("detection") == 1)).count()
    fp_total += df_with_combined_detection.filter((col("label") == 0) & (col("detection") == 1)).count()
    fn_total += df_with_combined_detection.filter((col("label") == 1) & (col("detection") == 0)).count()

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

    # Print metrics every 5 seconds
    if epoch_id % 5 == 0:
        print_metrics_5_seconds()

    # Print metrics every 10 seconds
    if epoch_id % 10 == 0:
        print_metrics_10_seconds()

# Print metrics every 5 seconds
def print_metrics_5_seconds():
    print("Metrics for the last 10 seconds:")
    print(f"Anomalies detected: {total_anomalies}")
    print(f"Events checked: {total_events}")

# Print metrics every 10 seconds
def print_metrics_10_seconds():
    print("Metrics since the start of processing:")
    print(f"Anomalies detected: {total_anomalies}")
    print(f"Eventsreceived: {total_events}")
    print(f"Accuracy of detecting abnormalities: {calculate_accuracy()}")
    print(f"Precision detection of anomalies: {calculate_precision()}")
    print(f"Recall detection of anomalies: {calculate_recall()}")

# Calculate accuracy
def calculate_accuracy():
    if total_events == 0:
        return 0.0
    else:
        return (tp_total + fn_total) / total_events

# Calculate precision
def calculate_precision():
    if tp_total + fp_total == 0:
        return 0.0
    else:
        return tp_total / (tp_total + fp_total)

# Calculate recall
def calculate_recall():
    if tp_total + fn_total == 0:
        return 0.0
    else:
        return tp_total / (tp_total + fn_total)

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "final-project") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", 100) \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

df.writeStream.trigger(processingTime='5 seconds').foreachBatch(process_batch).outputMode("update").option("checkpointLocation", "/checkpoints").start().awaitTermination()


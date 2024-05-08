# anomalies-detection-spark-streaming
A Spark program to detect anomalies in streaming data which utilizes Spark Structured Streaming library for real-time processing

This code ensemble is designed for anomaly detection in streaming data, utilizing Spark for real-time processing and Kafka for data ingestion and output. It includes modules for data preparation, anomaly detection, metric calculation, visualization, and data output.

Here is summary of what does each file do:

The "write-data-to-topic1.py" script reads data from a CSV file using Pandas, initializes a Kafka producer, converts each row of the dataframe to a JSON string, and sends it to a Kafka topic. It introduces a small delay to simulate streaming behavior.

The "print-metrics.py" script sets up a SparkSession for processing streaming data, defines a schema for the data, calculates anomalies in the streaming data using Z-score and mean of distances methods, and prints metrics such as anomalies detected, events checked, accuracy, precision, and recall at specified intervals.

The "write-output-to-a-topic2" script processes streaming data similarly to "print-metrics.py", but writes the output to a Kafka topic and saves it as CSV files. It also calculates anomalies using Z-score and mean of distances methods and combines the detection results before writing them to the output.

The "read-output-from-topic2-make-csv.py" script reads data from a Kafka topic as a streaming DataFrame, converts the value column into individual columns, and writes the streaming DataFrame to CSV files in the specified output directory.

The "plot.py" script reads a CSV file containing time-series data, separates normal and anomaly data based on labels and detections, creates a scatter plot of the time-series data with different markers for normal, anomaly, detected normal, and detected anomaly data points, and saves the plot as an image file.

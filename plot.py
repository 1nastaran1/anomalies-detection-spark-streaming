import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("timeseries.csv")

# Convert the 'date' column to datetime format
df['date'] = pd.to_datetime(df['date'])

# Separate data with label 1 and 0
anomaly_data = df[df['label'] == 1]
normal_data = df[df['label'] == 0]
anomaly_data_detected = df[df['detection'] == 1]
normal_data_detected = df[df['detection'] == 0]

# Create a scatter plot
plt.figure(figsize=(10, 6))

# Plot the normal and anomaly data
plt.scatter(normal_data['date'], normal_data['value'], label='Normal', color='blue')
plt.scatter(anomaly_data['date'], anomaly_data['value'], label='Anomaly', color='red')

# # Plot the detected normal and anomaly data
plt.scatter(normal_data_detected['date'], normal_data_detected['value'], label='Detected Normal', color='green', marker='x')
plt.scatter(anomaly_data_detected['date'], anomaly_data_detected['value'], label='Detected Anomaly', color='red', marker='x')

# Customize the plot
plt.title('Scatter Plot of Time Series Data')
plt.xlabel('Timestamp')
plt.ylabel('Value')
plt.legend()
plt.grid(False)

# Save the plot as an image file (e.g., PNG)
plt.savefig('detected2.png')

# Display the plot
plt.show()

# import the necessary libraries.

# Create a Spark session

# Define the schema for incoming JSON data

# Read streaming data from socket

# Parse JSON data into columns using the defined schema

# Convert timestamp column to TimestampType and add a watermark

# Perform windowed aggregation: sum of fare_amount over a 5-minute window sliding by 1 minute

# Extract window start and end times as separate columns

# Define a function to write each batch to a CSV file with column names

    # Save the batch DataFrame as a CSV file with headers included
    
# Use foreachBatch to apply the function to each micro-batch

query.awaitTermination()

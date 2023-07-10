# Import SparkSession
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .getOrCreate()

# Read from csv, create a DataFrame
df = spark.read.csv("Erasmus.csv", header=True)
#filtered_df = df.select("Sending Country Code", "Receiving Country Code").where(df["Sending Country Code"] == "FR")

# Shows every country how many students it received from the others country
filtered_df = df.groupby("Receiving Country Code", "Sending Country Code").count().sort("Receiving Country Code", "Sending Country Code")
# Print table
filtered_df.show(400)
# Import SparkSession
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .config("spark.jars", "mysql-connector-j-8.1.0.jar") \
      .getOrCreate()

# Read from csv, create a DataFrame
df = spark.read.csv("Erasmus.csv", header=True)
#filtered_df = df.select("Sending Country Code", "Receiving Country Code").where(df["Sending Country Code"] == "FR")

# Shows every country how many students it received from the others country
filtered_df = df.groupby("Receiving Country Code", "Sending Country Code").count().sort("Receiving Country Code", "Sending Country Code")

# Filtrare dupa coduri tari (daca sunt LV MK MT)
three_countries = ['LV', 'MK', 'MT']
three_df = filtered_df.filter(df['Receiving Country Code'].isin(three_countries))

# Print table
filtered_df.show(400)

# # Write to MySQL Table
three_df.write \
  .format("jdbc") \
  .option("driver", "com.mysql.cj.jdbc.Driver") \
  .option("url", "jdbc:mysql://localhost:3306/erasmus") \
  .option("dbtable", "Three_Countries") \
  .option("user", "root") \
  .option("password", "") \
  .mode("overwrite") \
  .save()

# store data regarding mobilities from a list of given receiving codes
def tabels_countries():
    for country in three_countries:
        table_name = country + "_Receiving"
        country_df = three_df .filter(three_df["Receiving Country Code"] == country).drop("Receiving Country Code")
        country_df.write \
            .format("jdbc") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("url", "jdbc:mysql://localhost:3306/erasmus") \
            .option("dbtable", table_name) \
            .option("user", "root") \
            .option("password", "") \
            .mode("overwrite") \
            .save()


tabels_countries()
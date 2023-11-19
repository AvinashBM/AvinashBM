from pyspark.sql import SparkSession
from datetime import date

# Set up SparkSession
spark = SparkSession.builder \
    .appName("PostgreSQL to Files") \
    .getOrCreate()

# PostgreSQL connection properties
jdbc_url = "jdbc:postgresql://your_postgres_host:your_postgres_port/your_database"
connection_properties = {
    "user": "your_username",
    "password": "your_password",
    "driver": "org.postgresql.Driver"
}

# Get current date
current_date = date.today()

# Query to fetch data from the PostgreSQL table
query = "SELECT * FROM your_table WHERE date_column = '{}'".format(current_date)

# Read data from PostgreSQL
df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)

# Write data to daily file
output_file = "/path/to/output/folder/data_{}.csv".format(current_date)
df.write.csv(output_file, header=True)

# Stop the SparkSession
spark.stop()

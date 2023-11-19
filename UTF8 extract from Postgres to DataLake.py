import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import psycopg2

# Connect to PostgreSQL database
conn = psycopg2.connect(
    host='your_host',
    port='your_port',
    dbname='your_database',
    user='your_user',
    password='your_password'
)

# Define the SQL query to fetch data from the PostgreSQL table
sql_query = 'SELECT * FROM your_table'

# Fetch data from PostgreSQL into a pandas DataFrame
df = pd.read_sql(sql_query, conn)

# Close the database connection
conn.close()

# Specify the Parquet file path
parquet_file_path = 'path_to_output_file.parquet'

# Convert DataFrame to a PyArrow table
table = pa.Table.from_pandas(df)

# Specify Parquet write options (including UTF-8 encoding)
write_options = pa.parquet.ParquetWriteOptions(use_dictionary=False, compression='snappy', data_page_version='2.0')

# Write the table to a Parquet file
pq.write_table(table, parquet_file_path, use_deprecated_int96_timestamps=True, write_options=write_options)

print('Parquet file created successfully.')

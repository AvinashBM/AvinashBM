import psycopg2
import pandas as pd
from azure.storage.blob import BlobServiceClient
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
# from datetime import datetime, timedelta
# from azure.datalake.store import DataLakeStoreAccount
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeFileClient

file_name = input("Enter the file name with .parquet extn : ")


def blob_file_upload(file_name):
    # Create a BlobServiceClient
    conn_str = f"DefaultEndpointsProtocol=https;AccountName=smdevswaptest;AccountKey=sLCb3nw//it7G/X0Y2q3QBtJYQJAfSMabNI1PpI+/3+iiXbw79HR7uNpjmFK8wi1GoXWSbGsiLBz+AStdfyojw==;EndpointSuffix=core.windows.net"
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)

    # Set the container name and create it if necessary
    container_name = "postgres-dump"
    container_client = blob_service_client.get_container_client(container_name)
    # container_client.create_container()

    # Set the Blob name
    blob_name = filename

    # Upload the Parquet file to Blob Storage
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(parquet_buffer, overwrite=True)
    print(f"File {file_name} uploaded successfully to Blob Storage! {blob_name}")


def dl_file_upload(file_name):
    # Define your Data Lake Storage account details
    account_name = "smpipelinedata"
    file_system_name = "reporting"
    destination_file_path = f"raw_swap_data/large_file/{file_name}"  # The path and filename for the destination file
    # local_file_path = "path/to/local/file.txt"  # The path and filename for the local file to be uploaded
    # account_key = "SrVxN/70Y6uiktX9XUIqypMiUaNy9yrh4DyRFcAd7+JueTmRFrKl5yThuNQhwfV3XisEfzEJSeQl+AStK0SDpg=="

    # Create a DataLakeFileClient object
    credential = DefaultAzureCredential()
    service_url = f"https://{account_name}.dfs.core.windows.net"
    file_system_url = f"{service_url}/{file_system_name}"
    file_client = DataLakeFileClient(file_system_url, credential)

    # Upload the file to the Data Lake Storage account
    with open(parquet_buffer, "rb") as file:
        file_client.upload_file(destination_file_path, file)

    print(f"File {file_name} uploaded successfully to DataLake! {destination_file_path}")


# Connect to the Postgres database dev2
# conn = psycopg2.connect(
#     host="pgserver-sm-dev2.postgres.database.azure.com",
#     database="smdev",
#     user="smadmin@pgserver-sm-dev2",
#     password="claystone@205",
# )

conn = psycopg2.connect(
    host="pgsql-sm-single-global-prod-001.postgres.database.azure.com",
    database="pgsqldb-single-global",
    user="smprod@pgsql-sm-single-global-prod-001",
    password="sunmobility@123",
)

# Specify the date
# start_date_string = input("Enter the START DATE for Swap Data Copy to Azure (YYYY-MM-DD): ")
# end_date_string = input("Enter the END DATE for Swap Data Copy to Azure (YYYY-MM-DD): ")
filename = input("Enter the filename for Swap Data with .parquet: ")

# Convert the date to a datetime object
# st_date_obj = datetime.strptime(start_date_string, '%Y-%m-%d')
# en_date_obj = datetime.strptime(end_date_string, '%Y-%m-%d')
#
# # Convert the datetime object to epoch time (in seconds)
# start_epoch = int(st_date_obj.timestamp()) * 1000
# end_epoch = int(en_date_obj.timestamp()) * 1000
#
# # Convert epoch dates to datetime objects
# start_date = (datetime.fromtimestamp(start_epoch))
# end_date = (datetime.fromtimestamp(end_epoch))

# Create a cursor object to execute SQL queries
cursor = conn.cursor()

query = '''select * from sm_wheeler_station_swap_records where sunmccu_time BETWEEN 1664582400000 AND 1675123200000'''

# Execute a SELECT query to fetch the data
cursor.execute(query)

# Fetch all the rows
rows = cursor.fetchall()
# Convert the fetched data into a DataFrame
df = pd.DataFrame(rows)

# Optionally, set column names based on the cursor.description
column_names = [desc[0] for desc in cursor.description]
df.columns = column_names

# Convert the DataFrame to a Parquet file in memory
parquet_buffer = BytesIO()
pq.write_table(pa.Table.from_pandas(df), parquet_buffer)
parquet_buffer.seek(0)

parquet_buffer = BytesIO()
table = pa.Table.from_pandas(df)
pq.write_table(table, parquet_buffer)
parquet_buffer.seek(0)


# blob_file_upload(file_name)
dl_file_upload(file_name)

# Close the cursor and connection
cursor.close()
conn.close()

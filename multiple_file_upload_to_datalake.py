import psycopg2
import pandas as pd
from azure.storage.blob import BlobServiceClient
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeFileClient
from datetime import datetime, timedelta

# PostgresSQL database connection parameters
db_host = 'pgsql-sm-single-global-prod-001.postgres.database.azure.com'
db_port = '5432'
db_name = 'pgsqldb-single-global'
db_user = 'smprod@pgsql-sm-single-global-prod-001'
db_password = 'sunmobility@123'

# Specify the date
start_date_string = input("Enter the START DATE for Swap Data Copy to Azure (YYYY-MM-DD): ")
end_date_string = input("Enter the END DATE for Swap Data Copy to Azure (YYYY-MM-DD): ")

# Convert the date to a datetime object
st_date_obj = datetime.strptime(start_date_string, '%Y-%m-%d')
en_date_obj = datetime.strptime(end_date_string, '%Y-%m-%d')

# Convert the datetime object to epoch time (in seconds)
start_epoch = int(st_date_obj.timestamp())
end_epoch = int(en_date_obj.timestamp())

# Convert epoch dates to datetime objects
start_date = (datetime.fromtimestamp(start_epoch))
end_date = (datetime.fromtimestamp(end_epoch))

# Connect to PostgreSQL database
conn = psycopg2.connect(
    host=db_host,
    port=db_port,
    database=db_name,
    user=db_user,
    password=db_password
)

# Create a cursor object to execute SQL queries
cursor = conn.cursor()

# def dl_file_upload(file_name, parquet_buffer):
#     # Define your Data Lake Storage account details
#     account_name = "smpipelinedata"
#     file_system_name = "reporting"
#     file_path = "raw_swap_data/daily_files/"
#     destination_file_path = f"raw_swap_data/daily_files/{file_name}"  # The path and filename for the destination file
#     # local_file_path = "path/to/local/file.txt"  # The path and filename for the local file to be uploaded
#     # account_key = "SrVxN/70Y6uiktX9XUIqypMiUaNy9yrh4DyRFcAd7+JueTmRFrKl5yThuNQhwfV3XisEfzEJSeQl+AStK0SDpg=="
#
#     # Create a DataLakeFileClient object
#     credential = DefaultAzureCredential()
#     service_url = f"https://{account_name}.dfs.core.windows.net"
#     file_system_url = f"{service_url}/{file_system_name}"
#     # file_client = DataLakeFileClient(file_system_url, credential,destination_file_path)
#     file_client = DataLakeFileClient(account_url=file_system_url, file_system_name=file_system_name, file_path=file_path, credential=credential)
#
#     # Upload the file to the Data Lake Storage account
#     with open(parquet_buffer, "rb") as file:
#         file_client.upload_file(destination_file_path, file)
#
#     print(f"File {file_name} uploaded successfully to DataLake! {destination_file_path}")



# Iterate over the date range
current_date = start_date
while current_date <= end_date:
    # Define the file name for the current date
    # blob_name_csv = f"data_{current_date.strftime('%Y-%m-%d')}.csv"
    # blob_name_json = f"data_{current_date.strftime('%Y-%m-%d')}.json"
    file_name_par = f"data_{current_date.strftime('%Y-%m-%d')}.parquet"

    fst_time = datetime.now()

    # Calculate the start and end epoch for the current date
    start_epoch = int(current_date.timestamp()) * 1000
    end_epoch = int((current_date + timedelta(days=1)).timestamp()) * 1000

    # Execute SQL query to fetch data for the current date
    # query = f"SELECT * FROM sm_wheeler_station_swap_records WHERE sunmccu_time >= {start_epoch} AND sunmccu_time < {end_epoch}"
    query = f"SELECT * FROM sm_wheeler_station_swap_records WHERE sunmccu_time >= 1672511400000 AND sunmccu_time < 1673116199000"

    # Execute a SELECT query to fetch the data
    cursor.execute(query)

    # Fetch all the rows
    rows = cursor.fetchall()
    df = pd.DataFrame(rows)
    # Close the database connection
    cursor.close()

    # Convert the results to a PyArrow Table
    table = pa.Table.from_pandas(rows)

    # Write the PyArrow Table to a Parquet file
    pq.write_table(table, "temp.parquet")

    #Define your Data Lake Storage account details
    account_name = "smpipelinedata"
    file_system_name = "reporting"
    file_path = "raw_swap_data/daily_files/"
    destination_file_path = f"raw_swap_data/daily_files/{file_name_par}"  # The path and filename for the destination file
    # local_file_path = "path/to/local/file.txt"  # The path and filename for the local file to be uploaded
    # account_key = "SrVxN/70Y6uiktX9XUIqypMiUaNy9yrh4DyRFcAd7+JueTmRFrKl5yThuNQhwfV3XisEfzEJSeQl+AStK0SDpg=="
    # Create a DataLakeFileClient object
    credential = DefaultAzureCredential()
    service_url = f"https://{account_name}.dfs.core.windows.net"
    file_system_url = f"{service_url}/{file_system_name}"

    # Upload the Parquet file to Azure Data Lake Storage
    with open("temp.parquet", "rb") as file:
        file_client.upload_file(destination_file_path, file)

    # blob_file_upload(file_name)
    # dl_file_upload(file_name_par, parquet_buffer)


    fet_time = datetime.now()
    duration = fet_time - fst_time

    # # Create a blob client and upload the JSON data
    # container_client = blob_service_client.get_container_client(container_name)
    # blob_client = container_client.get_blob_client(blob_name_json)
    # blob_client.upload_blob(json_string, overwrite=True)

    print(f"Data for {current_date.strftime('%Y-%m-%d')} of size {file_size_csv_mb:.2f} MB copied to Azure Blob Storage {container_name} successfully with time duartion of {duration.seconds} seconds")

    # Move to the next date
    current_date += timedelta(days=1)

# Close the cursor and database connection
# cur.close()
conn.close()

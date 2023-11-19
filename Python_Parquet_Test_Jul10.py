import psycopg2
from pyarrow import parquet
import pyarrow as pa
import pandas as pd
from azure.storage.filedatalake import DataLakeFileClient
import pyarrow.parquet as pq

# Azure PostgreSQL database settings
host = 'pgsql-sm-single-global-prod-001.postgres.database.azure.com'
port = 5432
database = 'pgsqldb-single-global'
username = 'smprod@pgsql-sm-single-global-prod-001'
password = 'sunmobility@123'

# table_name = "sm_wheeler_station_swap_records"
table_name = "sm_wheeler_station_admin_swap_metrics"

# Connect to Azure PostgreSQL database
conn = psycopg2.connect(database=database, host=host, user=username, password=password)

# Fetch table column names
cursor = conn.cursor()
cursor.execute(f"SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'")
column_names = [column[0] for column in cursor.fetchall()]
cursor.close()

# Fetch table data
cursor = conn.cursor()
#cursor.execute(f"SELECT * FROM {table_name} WHERE sunmccu_time between 1672511400000 AND 1673116199000")
cursor.execute(f"select * from {table_name} where sunmccu_time between 1672511400000 and 1673116199000")
data = cursor.fetchall()
cursor.close()

parquet_file_name = "sm_admin_json.parquet"

# Create Parquet file
parquet_table = pa.Table.from_pandas(pd.DataFrame(data, columns=column_names))
parquet.write_table(parquet_table, parquet_file_name)


# parquet_table[sunmccu_data] = pd.concat([parquet_table[sunmccu_data], parquet_table["col_json"].apply(pd.Series)], axis=1).drop('col_json ', axis=1)
#
# parquet.write_table(parquet_table, 'test3.parquet')
#
# account_name = 'smpipelinedata'
# container_name = 'reporting'
# access_key = 'SrVxN/70Y6uiktX9XUIqypMiUaNy9yrh4DyRFcAd7+JueTmRFrKl5yThuNQhwfV3XisEfzEJSeQl+AStK0SDpg=='
#
# # Set the path and filename for the Parquet file you want to upload
# local_file_path = 'parquet_file_name'
# remote_file_path = 'reporting/file.parquet'
#
# # Load the Parquet file using PyArrow
# parquet_table = pq.read_table(local_file_path)
#
# # Convert the Parquet table to a pandas DataFrame (optional)
# data_frame = parquet_table.to_pandas()
#
# # Create a DataLakeFileClient object
# file_client = DataLakeFileClient(account_url=f"https://{account_name}.dfs.core.windows.net", file_system_name=container_name, file_path=remote_file_path, credential=access_key)
#
# # Upload the Parquet file to Azure Data Lake Storage Gen2
# with file_client.create_file() as file:
#     # Write the Parquet table to the file
#     pq.write_table(parquet_table, file)
#     # If you converted the table to a DataFrame, you can also write it as follows:
#     # data_frame.to_parquet(file)
#
# # Create a DataLakeStoreAccount object
# account = DataLakeStoreAccount(account_name=account_name, filesystem=container_name, account_key=access_key)
#
# # Create or overwrite the file in the Azure Data Lake Storage Gen2 container
# with account.create_file(remote_file_path, overwrite=True) as file:
#     # Write the Parquet table to the file
#     pq.write_table(parquet_table, file)
#     # If you converted the table to a DataFrame, you can also write it as follows:
#     # data_frame.to_parquet(file)

print('Parquet file uploaded successfully.')

# Clean up
conn.close()

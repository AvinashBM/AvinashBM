import psycopg2
from azure.storage.blob import BlobServiceClient

# PostgreSQL database connection parameters
db_host = 'pgserver-sm-dev2.postgres.database.azure.com'
db_port = '5432'
db_name = 'smdev'
db_user = 'smadmin@pgserver-sm-dev2'
db_password = 'claystone@205'

# Azure Blob Storage connection parameters
azure_storage_connection_string = "DefaultEndpointsProtocol=https;AccountName=smdevswaptest;AccountKey=sLCb3nw//it7G/X0Y2q3QBtJYQJAfSMabNI1PpI+/3+iiXbw79HR7uNpjmFK8wi1GoXWSbGsiLBz+AStdfyojw==;EndpointSuffix=core.windows.net"
container_name = 'postgres-dump'
blob_name = 'smdevswaptest'

# Connect to PostgreSQL database
conn = psycopg2.connect(
    host=db_host,
    port=db_port,
    database=db_name,
    user=db_user,
    password=db_password
)

# Create a cursor object to execute SQL queries
cur = conn.cursor()

# Execute SQL query to fetch data from the table
cur.execute("SELECT * FROM sm_wheeler_station_swap_records order by sunmccu_time desc limit 1000")

# Fetch all the rows from the query result
rows = cur.fetchall()

# Close the cursor and database connection
cur.close()
conn.close()

# Create a connection to Azure Blob Storage
blob_service_client = BlobServiceClient.from_connection_string(azure_storage_connection_string)

# Create a new blob client
blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

# Convert rows to a string format (CSV in this example)
data = '\n'.join([','.join(map(str, row)) for row in rows])

# Upload the data to Azure Blob Storage
blob_client.upload_blob(data)

print("Data copied to Azure Blob Storage successfully.")

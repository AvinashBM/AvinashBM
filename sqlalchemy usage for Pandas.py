import pandas as pd
from import create_engine

# PostgreSQL connection details
db_url = 'postgresql://username:password@host:port/database_name'
table_name = 'your_table_name'

# Create a SQLAlchemy engine
engine = create_engine(db_url)

# Retrieve data from PostgreSQL using pandas
query = f'SELECT * FROM {table_name}'
df = pd.read_sql(query, engine)

# Process the retrieved data as needed
# ...

# Display the DataFrame
print(df)


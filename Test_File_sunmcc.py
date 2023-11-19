import pyarrow.parquet as pq
import pandas as pd
from fastparquet import ParquetFile

parquet_file_path = 'sm_admin_json.parquet'
new_parquet_file_path = 'sm_admin_json_head5.parquet'
# json_column = "sunmccu_data"

df = ParquetFile(parquet_file_path)
for df in df.iter_row_groups():
    # print(df.head(n=10))
    print(df['sunmccu_data.adminSwapEvents'][10])
    # df.to_csv('dftest.csv',index=False)
    # new_columns = df[json_column].apply(pd.Series)
    # df = pd.concat([df, new_columns], axis=1).drop(json_column, axis=1)
    # df.to_parquet(new_parquet_file_path,append=True)

parquet_table = pq.read_table(parquet_file_path)
data_frame = parquet_table.to_pandas()

# new_columns = data_frame[json_column].apply(pd.Series)
# data_frame = pd.concat([data_frame, new_columns], axis=1).drop(json_column, axis=1)

# You can access and manipulate the new columns in the DataFrame
# print(data_frame.head())

data_frame.head().to_parquet(new_parquet_file_path)

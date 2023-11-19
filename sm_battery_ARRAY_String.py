import os
import pandas as pd

def read_and_convert_to_string(folder_path):
    for folder in os.listdir(folder_path):
        REF_PATH = os.path.join(folder_path, folder)
        print(REF_PATH)
        # Get a list of all files in the folder
        file_list = [f for f in os.listdir(REF_PATH) if os.path.isfile(os.path.join(REF_PATH, f))]
        # Initialize an empty list to store data from all files
        # dataframes = []
        # os.mkdir(f"D:\Avinash.BM\OneDrive - SUN Mobility\Desktop\Main\PycharmProjects\Output\{folder}")
        # os.mkdir(f"D:\Avinash.BM\OneDrive - SUN Mobility\Desktop\Main\PycharmProjects\Output")
        # Read each Parquet file, convert columns to string, and append to the dataframes list
        for file_name in file_list:

            file_path = os.path.join(REF_PATH, file_name)
            df = pd.read_parquet(file_path)
            # print(file_name, file_path)
            # df = df.columns('vibration').astype(str)  # Convert all columns to string datatype
            list_conv = ['vibration','cellvoltages','celltemperatures','batterydisconnectreason','dischargestopreason','controllerfaults1','controllerfaults2','controllerfaults3','controllerfaults4','controllerfaults5','fault1','fault2','fault3','fault4','fault5','fault6','fault7','fault8','fault9','fault10','fault11','fault12','fault13','fault14','fault15','fault16','fault17','fault18','fault19','fault20','digitalinputstatus','digitaloutputstatus','fault21','fault22','fault23','fault24','fault25','fault26','fault27','fault28','fault29','fault30','quarantine_reasons']
            df[list_conv] = df[list_conv].astype(str)
            # dataframes.append(df)
            # print(df.info())
            # os.mkdir(f"D:\Avinash.BM\OneDrive - SUN Mobility\Desktop\Main\PycharmProjects\Output")

            file_path_dest = os.path.join(f"D:\Avinash.BM\OneDrive - SUN Mobility\Desktop\Main\PycharmProjects\Output", file_name)
            df.to_parquet(file_path_dest)

    # Concatenate all dataframes into a single dataframe
    # combined_df = pd.concat(dataframes, ignore_index=True)

    return True

# Replace 'folder_path' with the path to the folder containing the Parquet files
folder_path = 'D:\Avinash.BM\OneDrive - SUN Mobility\Desktop\Main\PycharmProjects\Battery_Data_Archival'
result_df = read_and_convert_to_string(folder_path)
# print(result_df.head())
# result_df = result_df.to_frame()
#
# result_df.to_parquet('INSMOMAH0106G230A312_Combined.parquet')

# Now 'result_df' contains the data from all Parquet files in the folder with all columns in string format.

import os
import pandas as pd
from simpledbf import Dbf5

def generate_dbf_file_names(dbf_time_periods):
    return [f"LOAD{tod}_FINAL.dbf" for tod in dbf_time_periods]

def handle_merged_header(excel_file, sheet_name, extra_columns):
    df = pd.read_excel(
        excel_file,
        sheet_name=sheet_name,
        header=None,
        skiprows=1)
    header = df.iloc[0]
    df = df[1:]
    df.columns = header
    return df[extra_columns]


def filter_and_aggregate(
        excel_file,
        sheet_name,
        dbf_directory,
        dbf_files,
        column_names,
        time_periods,
        extra_columns):
    dbf_files = generate_dbf_file_names(time_periods)    
    excel_df = handle_merged_header(excel_file, sheet_name, extra_columns)
    base_df = excel_df.reset_index(drop=True)

    for col in time_periods + ['Daily']:
        base_df[col] = 0

    for dbf_file, output_column in zip(dbf_files, time_periods):
        dbf_file_path = os.path.join(dbf_directory, dbf_file)
        dbf = Dbf5(dbf_file_path)
        dbf_df = dbf.to_dataframe()
        dbf_df = dbf_df[column_names]

        dbf_dict = {(row['A'], row['B']): row for index,
                    row in dbf_df.iterrows()}

        for index, row in base_df.iterrows():
            key = (row['A'], row['B'])
            if key in dbf_dict:
                dbf_row = dbf_dict[key]

                for extra_col in column_names:
                    if extra_col not in ['A', 'B', 'V_1']:
                        base_df.at[index, extra_col] = dbf_row[extra_col]

                base_df.at[index, output_column] = dbf_row['V_1']

                base_df.at[index, 'Daily'] += dbf_row['V_1']

    return base_df
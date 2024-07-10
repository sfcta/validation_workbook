import os
import tomllib

import pandas as pd
import shapefile

# we should be importing functions in this file into transit.py instead
# HOTFIX TODO pass results of read_transit_assignments() directly as arg
from transit import transit_assignment_filepaths

with open("transit.toml", "rb") as f:
    config = tomllib.load(f)

model_run_dir = config["directories"]["model_run"]
transit_assignments = transit_assignment_filepaths(model_run_dir)

WORKING_FOLDER = config["directories"]["transit_input_dir"]
OUTPUT_FOLDER = config["directories"]["transit_output_dir"]
MUNI_OBS = os.path.join(WORKING_FOLDER, config["transit"]["Transit_Templet"])
model_BART_SL = os.path.join(OUTPUT_FOLDER, config["output"]["model_BART_SL"])


HWY_SCREENS = {
    "SamTrans": [
        [40029, 7732, 52774, 33539, 51113, 21584, 50995],  # inbound
        [52118, 52264, 21493, 33737, 22464, 21522, 20306],  # outbound
        ["SamTrans", "Countyline", "SamTrans", "Local Bus"],
    ],
    "GG Transit": [
        [8318, 8315],  # inbound
        [8338, 8339],  # outbound
        ["Golden Gate Transit", "Golden Gate", "Golden Gate Transit", "Local Bus"],
    ],
    "GG Ferry": [
        [15503, 15608, 15503, 15608, 15502],  # inbound
        [15501, 15600, 15601, 15601, 15600],  # outbound
        ["Ferry", "Golden Gate", "Golden Gate Ferry", "Ferry"],
    ],
    "CalTrain": [
        [14659, 14659, 14661, 14660, 14661, 14660],  # inbound
        [14658, 14655, 14655, 14655, 14656, 14656],  # outbound
        ["Caltrain", "Countyline", "CalTrain", "Premium"],
    ],
    "AC transit": [
        [52833, 52832],  # inbound
        [52495, 52494],  # outbound
        ["AC Transit", "Transbay", "AC Transit", "Premium"],
    ],
}


def read_dbf_and_groupby_sum(dbf_file_path, system_filter, groupby_columns, sum_column):
    """
    Reads a DBF file, filters by system, groups by specified columns,
    and calculates sum of a specified column.

    Parameters:
    dbf_file_path (str): The path to the DBF file.
    system_filter (str): The value to filter by on the 'SYSTEM' column.
    groupby_columns (list): The list of columns to group by.
    sum_column (str): The column on which to calculate the sum.

    Returns:
    DataFrame: Pandas DataFrame with the groupby and sum applied.
    """
    # Create a shapefile reader object
    sf = shapefile.Reader(dbf_file_path)

    # Extract fields and records from the DBF file
    fields = [x[0] for x in sf.fields][1:]
    records = sf.records()

    # Create a DataFrame using the extracted data
    df = pd.DataFrame(columns=fields, data=records)

    # Filter the DataFrame based on the 'SYSTEM' column
    filtered_df = df[df["SYSTEM"] == system_filter]

    # Group by the specified columns and sum the specified column
    grouped_sum = filtered_df.groupby(groupby_columns)[sum_column].sum()

    # Resetting index to convert it back to a DataFrame
    grouped_sum_df = grouped_sum.reset_index()

    return grouped_sum_df


def process_data(file_name, system, TOD, A, B, Screenline, Operator, Mode):
    # Create DataFrames for IB and OB
    ST_IB = pd.DataFrame({"A": A, "B": B})
    ST_OB = pd.DataFrame({"A": B, "B": A})

    # Read the DBF file and group by 'A' and 'B' while summing 'AB_VOL'
    ST_TOD = read_dbf_and_groupby_sum(file_name, system, ["A", "B"], "AB_VOL")

    # Filter rows for IB and calculate the sum of 'AB_VOL'
    ST_TOD_IB = ST_TOD[(ST_TOD["A"].isin(A)) & (ST_TOD["B"].isin(B))]
    IB_sum = ST_TOD_IB["AB_VOL"].sum()

    # Filter rows for OB and calculate the sum of 'AB_VOL'
    ST_TOD_OB = ST_TOD[(ST_TOD["A"].isin(B)) & (ST_TOD["B"].isin(A))]
    OB_sum = ST_TOD_OB["AB_VOL"].sum()

    # Create DataFrames for IB and OB results
    data_IB = {
        "Screenline": [Screenline],
        "Direction": ["IB"],
        "TOD": [TOD],
        "Ridership": [IB_sum],
        "Operator": [Operator],
        "Mode": [Mode],
    }
    data_OB = {
        "Screenline": [Screenline],
        "Direction": ["OB"],
        "TOD": [TOD],
        "Ridership": [OB_sum],
        "Operator": [Operator],
        "Mode": [Mode],
    }
    ST_TOD_IB1 = pd.DataFrame(data_IB)
    ST_TOD_OB1 = pd.DataFrame(data_OB)

    # Concatenate IB and OB DataFrames
    ST_TOD1 = pd.concat([ST_TOD_IB1, ST_TOD_OB1])

    return ST_TOD1


def screen_df(transit_assignments, HWY_SCREENS):
    df_total = []
    for i in HWY_SCREENS.keys():
        df_i = []
        for path in transit_assignments:
            period = path[-6:-4]
            df = process_data(
                path,
                HWY_SCREENS[i][2][0],
                period,
                HWY_SCREENS[i][0],
                HWY_SCREENS[i][1],
                HWY_SCREENS[i][2][1],
                HWY_SCREENS[i][2][2],
                HWY_SCREENS[i][2][3],
            )
            df_i.append(df)
        SC = pd.concat(df_i)
        SC["Key"] = SC["Screenline"] + SC["Operator"] + SC["TOD"] + SC["Direction"]
        SC = SC[
            ["Screenline", "Direction", "TOD", "Key", "Ridership", "Operator", "Mode"]
        ]
        SC = SC.sort_values(by="Direction").reset_index(drop=True)
        df_total.append(SC)
    model_Screenlines = pd.concat(df_total)
    return model_Screenlines


model_Screenlines = screen_df(transit_assignments, HWY_SCREENS)
BART_Screenlines = pd.read_csv(model_BART_SL)
BART_Screenlines["Operator"] = "BART"
BART_Screenlines["Mode"] = "BART"
BART_Screenlines["Key"] = (
    BART_Screenlines["Screenline"]
    + BART_Screenlines["Operator"]
    + BART_Screenlines["TOD"]
    + BART_Screenlines["Direction"]
)
model_SL = pd.concat([BART_Screenlines, model_Screenlines])
model_SL.to_csv(os.path.join(OUTPUT_FOLDER, "model_SL.csv"), index=False)

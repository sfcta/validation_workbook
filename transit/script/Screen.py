import os
import tomllib

import pandas as pd

# we should be importing functions in this file into transit.py instead
# HOTFIX TODO pass results of read_transit_assignments() directly as arg
from transit import read_dbf_and_groupby_sum, transit_assignment_filepaths

with open("transit.toml", "rb") as f:
    config = tomllib.load(f)

model_run_dir = config["directories"]["model_run"]

WORKING_FOLDER = config["directories"]["transit_input_dir"]
OUTPUT_FOLDER = config["directories"]["transit_output_dir"]
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


def process_data(file_name, system, TOD, A, B, Screenline, Operator, Mode):
    # ST_IB, ST_OB: HOTFIX commented out for now; TODO remove if unused
    # # Create DataFrames for IB and OB
    # ST_IB = pd.DataFrame({"A": A, "B": B})
    # ST_OB = pd.DataFrame({"A": B, "B": A})

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


def screen_df(transit_assignment_filepaths, HWY_SCREENS):
    df_total = []
    for i in HWY_SCREENS.keys():
        df_i = []
        for period, path in transit_assignment_filepaths.itmes():
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


model_Screenlines = screen_df(
    transit_assignment_filepaths(model_run_dir=model_run_dir), HWY_SCREENS
)
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

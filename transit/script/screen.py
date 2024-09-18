import pandas as pd
import tomllib
from transit_function import read_dbf_and_groupby_sum, read_transit_assignments

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


def process_data(file_name, system, A, B, Screenline, Operator, Mode):
    # Read the DBF file and group by 'A' and 'B' while summing 'AB_VOL'
    ST_TOD = read_dbf_and_groupby_sum(file_name, system, ["A", "B", "TOD"], "AB_VOL")

    # Filter rows for IB and calculate the sum of 'AB_VOL"
    ST_TOD_IB = ST_TOD[(ST_TOD["A"].isin(A)) & (ST_TOD["B"].isin(B))]
    IB_sum = ST_TOD_IB.groupby("TOD")["AB_VOL"].sum().reset_index()
    IB_sum["Screenline"] = Screenline
    IB_sum["Direction"] = "IB"
    IB_sum["Operator"] = Operator
    IB_sum["Mode"] = Mode

    # Filter rows for OB and calculate the sum of 'AB_VOL'
    ST_TOD_OB = ST_TOD[(ST_TOD["A"].isin(B)) & (ST_TOD["B"].isin(A))]
    OB_sum = ST_TOD_OB.groupby("TOD")["AB_VOL"].sum().reset_index()
    OB_sum["Screenline"] = Screenline
    OB_sum["Direction"] = "OB"
    OB_sum["Operator"] = Operator
    OB_sum["Mode"] = Mode


    ST_TOD = pd.concat([IB_sum, OB_sum])
    ST_TOD = ST_TOD.rename(columns={"AB_VOL":"Ridership"})

    return ST_TOD


def screen_df(file_name, HWY_SCREENS):
    screenline_total = []
    for i in HWY_SCREENS.keys():
        screenline = process_data(
                file_name,
                HWY_SCREENS[i][2][0],
                HWY_SCREENS[i][0],
                HWY_SCREENS[i][1],
                HWY_SCREENS[i][2][1],
                HWY_SCREENS[i][2][2],
                HWY_SCREENS[i][2][3],
            )
        screenline["Key"] = screenline["Screenline"] + screenline["Operator"] + screenline["TOD"] + screenline["Direction"]
        screenline = screenline[
            ["Screenline", "Direction", "TOD", "Key", "Ridership", "Operator", "Mode"]
        ]
        screenline = screenline.sort_values(by="Direction").reset_index(drop=True)
        screenline_total.append(screenline)
    model_Screenlines = pd.concat(screenline_total)
    return model_Screenlines

def concat_final_SL(file_name, output_transit_dir, model_BART_SL, model_SL):
    model_Screenlines = screen_df(file_name,HWY_SCREENS)
    BART_Screenlines = pd.read_csv(output_transit_dir / model_BART_SL)
    BART_Screenlines["Operator"] = "BART"
    BART_Screenlines["Mode"] = "BART"
    BART_Screenlines["Key"] = (
        BART_Screenlines["Screenline"]
        + BART_Screenlines["Operator"]
        + BART_Screenlines["TOD"]
        + BART_Screenlines["Direction"]
    )
    model_SL_df = pd.concat([BART_Screenlines, model_Screenlines]).reset_index(drop=True)
    model_SL_df.to_csv(output_transit_dir / model_SL, index=False)
    
if __name__ == "__main__":
    with open("transit.toml", "rb") as f:
        config = tomllib.load(f)
        
    transit_line_rename_filepath = (
        config["directories"]["resources"] / config["transit"]["line_rename_filename"]
    ) 
    transit_validation_2019_alfaro_filepath = config["transit"][
        "transit_validation_2019_alfaro_filepath"
    ]
    model_run_dir = config["directories"]["model_run"]
    model_BART_SL = config["output"]["model_BART_SL"]
    model_SL = config["output"]["model_SL"]
    output_dir = model_run_dir / "validation_workbook" / "output"
    output_transit_dir = output_dir / "transit"
    output_transit_dir.mkdir(parents=True, exist_ok=True)
    time_periods = ["EA", "AM", "MD", "PM", "EV"]
    dbf_file = read_transit_assignments(model_run_dir, time_periods)
    
    concat_final_SL(dbf_file, output_transit_dir, model_BART_SL, model_SL)

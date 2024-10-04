import pandas as pd
import tomllib
from utils import read_dbf_and_groupby_sum, read_transit_assignments

# HWY_SCREENS = {
#     "SamTrans": [
#         [40029, 7732, 52774, 33539, 51113, 21584, 50995],  # inbound
#         [52118, 52264, 21493, 33737, 22464, 21522, 20306],  # outbound
#         ["SamTrans", "Countyline", "SamTrans", "Local Bus"],
#     ],
#     "GG Transit": [
#         [8318, 8315],  # inbound
#         [8338, 8339],  # outbound
#         ["Golden Gate Transit", "Golden Gate", "Golden Gate Transit", "Local Bus"],
#     ],
#     "GG Ferry": [
#         [15503, 15608, 15503, 15608, 15502],  # inbound
#         [15501, 15600, 15601, 15601, 15600],  # outbound
#         ["Ferry", "Golden Gate", "Golden Gate Ferry", "Ferry"],
#     ],
#     "CalTrain": [
#         [14659, 14659, 14661, 14660, 14661, 14660],  # inbound
#         [14658, 14655, 14655, 14655, 14656, 14656],  # outbound
#         ["Caltrain", "Countyline", "CalTrain", "Premium"],
#     ],
#     "AC transit": [
#         [52833, 52832],  # inbound
#         [52495, 52494],  # outbound
#         ["AC Transit", "Transbay", "AC Transit", "Premium"],
#     ],
# }


def group_screenline_ridership(combined_gdf, system, A, B, Screenline, Operator, Mode):
    # Read the DBF file and group by 'A' and 'B' while summing 'AB_VOL'
    screenline_total_tod = read_dbf_and_groupby_sum(
        combined_gdf, system, ["A", "B", "TOD"], "AB_VOL"
    )

    # Filter rows for IB and calculate the sum of 'AB_VOL"
    screenline_total_tod_ib = screenline_total_tod[
        (screenline_total_tod["A"].isin(A)) & (screenline_total_tod["B"].isin(B))
    ]
    IB_sum = screenline_total_tod_ib.groupby("TOD")["AB_VOL"].sum().reset_index()
    IB_sum["Screenline"] = Screenline
    IB_sum["Direction"] = "IB"
    IB_sum["Operator"] = Operator
    IB_sum["Mode"] = Mode

    # Filter rows for OB and calculate the sum of 'AB_VOL'
    screenline_total_tod_ob = screenline_total_tod[
        (screenline_total_tod["A"].isin(B)) & (screenline_total_tod["B"].isin(A))
    ]
    OB_sum = screenline_total_tod_ob.groupby("TOD")["AB_VOL"].sum().reset_index()
    OB_sum["Screenline"] = Screenline
    OB_sum["Direction"] = "OB"
    OB_sum["Operator"] = Operator
    OB_sum["Mode"] = Mode

    screenline_total = pd.concat([IB_sum, OB_sum])
    screenline_total = screenline_total.rename(columns={"AB_VOL": "Ridership"})

    return screenline_total


def process_screenline_data(
    combined_gdf, SamTrans, GG_Transit, GG_Ferry, CalTrain, AC_transit
):
    """
    Processes the screenline data by concat grouping ridership data of each screenline.
    """
    HWY_SCREENS = {
        "SamTrans": SamTrans,
        "GG Transit": GG_Transit,
        "GG Ferry": GG_Ferry,
        "CalTrain": CalTrain,
        "AC transit": AC_transit,
    }
    screenline_total = []
    for i in HWY_SCREENS.keys():
        screenline = group_screenline_ridership(
            combined_gdf,
            HWY_SCREENS[i][2][0],
            HWY_SCREENS[i][0],
            HWY_SCREENS[i][1],
            HWY_SCREENS[i][2][1],
            HWY_SCREENS[i][2][2],
            HWY_SCREENS[i][2][3],
        )
        screenline["Key"] = (
            screenline["Screenline"]
            + screenline["Operator"]
            + screenline["TOD"]
            + screenline["Direction"]
        )
        screenline = screenline[
            ["Screenline", "Direction", "TOD", "Key", "Ridership", "Operator", "Mode"]
        ]
        screenline = screenline.sort_values(by="Direction").reset_index(drop=True)
        screenline_total.append(screenline)
    model_Screenlines = pd.concat(screenline_total)
    return model_Screenlines


def save_final_screenline_data(
    combined_gdf,
    output_transit_dir,
    model_BART_Screenline,
    model_Screenline,
    SamTrans,
    GG_Transit,
    GG_Ferry,
    CalTrain,
    AC_transit,
):
    HWY_SCREENS = {
        "SamTrans": SamTrans,
        "GG Transit": GG_Transit,
        "GG Ferry": GG_Ferry,
        "CalTrain": CalTrain,
        "AC transit": AC_transit,
    }
    model_Screenlines = process_screenline_data(
        combined_gdf, SamTrans, GG_Transit, GG_Ferry, CalTrain, AC_transit
    )
    BART_Screenlines = pd.read_csv(output_transit_dir / model_BART_Screenline)
    BART_Screenlines["Operator"] = "BART"
    BART_Screenlines["Mode"] = "BART"
    BART_Screenlines["Key"] = (
        BART_Screenlines["Screenline"]
        + BART_Screenlines["Operator"]
        + BART_Screenlines["TOD"]
        + BART_Screenlines["Direction"]
    )
    model_Screenline_df = pd.concat([BART_Screenlines, model_Screenlines]).reset_index(
        drop=True
    )
    model_Screenline_df.to_csv(output_transit_dir / model_Screenline, index=False)


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
    model_BART_Screenline = config["bart"]["model_BART_Screenline"]
    model_Screenline = config["screenline"]["model_Screenline"]
    GG_Transit = config["screenline"]["GG_Transit"]
    SamTrans = config["screenline"]["SamTrans"]
    GG_Ferry = config["screenline"]["GG_Ferry"]
    CalTrain = config["screenline"]["CalTrain"]
    AC_transit = config["screenline"]["AC_transit"]
    output_dir = model_run_dir / "validation_workbook" / "output"
    output_transit_dir = output_dir / "transit"
    output_transit_dir.mkdir(parents=True, exist_ok=True)
    time_periods = ["EA", "AM", "MD", "PM", "EV"]
    combined_gdf = read_transit_assignments(model_run_dir, time_periods)

    save_final_screenline_data(
        combined_gdf,
        output_transit_dir,
        model_BART_Screenline,
        model_Screenline,
        SamTrans,
        GG_Transit,
        GG_Ferry,
        CalTrain,
        AC_transit,
    )

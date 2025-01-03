import pandas as pd
import tomllib
from utils import (
    dataframe_to_markdown,
    format_dataframe,
    read_dbf_and_groupby_sum,
    read_transit_assignments,
    time_periods,
)


# Get Observed data from NTD
def obs_ntd_table(transit_input_dir, observed_NTD):
    obs_NTD_df = pd.read_csv(transit_input_dir / observed_NTD)
    obs_NTD_df = obs_NTD_df[["operator", "annual_upt", "average weekday_upt"]]
    obs_NTD_df["average weekday_upt"] = obs_NTD_df.apply(
        lambda row: round(row["annual_upt"] / 261)
        if pd.isna(row["average weekday_upt"])
        else row["average weekday_upt"],
        axis=1,
    )
    obs_NTD_avgupt = obs_NTD_df[["operator", "average weekday_upt"]]
    obs_NTD_avgupt = obs_NTD_avgupt.rename(
        columns={"operator": "Operator", "average weekday_upt": "Observed"}
    )
    return obs_NTD_avgupt


def calcualte_weekday_upt(transit_input_dir, observed_NTD):
    obs_NTD_df = pd.read_csv(transit_input_dir / observed_NTD)
    obs_NTD_avgupt = obs_ntd_table(transit_input_dir, observed_NTD)
    ratio = obs_NTD_df[["operator", "annual_upt", "average weekday_upt"]]
    ratio["ratio"] = ratio["annual_upt"]/ratio["average weekday_upt"].fillna(261)
    
    AC_Transbay = (
        obs_NTD_df[obs_NTD_df["operator"] == "AC-Transit"]["commuter_bus_total"] / ratio[ratio["operator"] == "AC-Transit" ]["ratio"]
    )
    AC_Eastbay = obs_NTD_df[obs_NTD_df["operator"] == "AC-Transit"]["bus_total"] / ratio[ratio["operator"] == "AC-Transit" ]["ratio"]
    AC = {
        "Operator": ["AC Transbay", "AC Eastbay"],
        "Observed": [AC_Transbay.mean(), AC_Eastbay.mean()],
    }
    df_AC = pd.DataFrame(AC)

    GGT_bus = obs_NTD_df[obs_NTD_df["operator"] == "GG Transit"]["bus_total"] /  ratio[ratio["operator"] == "GG Transit" ]["ratio"]
    GGT_Ferry = obs_NTD_df[obs_NTD_df["operator"] == "GG Transit"]["ferry_total"] / ratio[ratio["operator"] == "GG Transit" ]["ratio"]
    GG = {
        "Operator": ["GGT-Bus", "GGT-Ferry"],
        "Observed": [GGT_bus.mean(), GGT_Ferry.mean()],
    }
    GG_Transit = pd.DataFrame(GG)

    MUNI_Bus = obs_NTD_df[obs_NTD_df["operator"] == "MUNI"]["bus_total"] / ratio[ratio["operator"] == "MUNI" ]["ratio"]
    MUNI_Rail = obs_NTD_df[obs_NTD_df["operator"] == "MUNI"]["light_rail_total"] / ratio[ratio["operator"] == "MUNI" ]["ratio"]
    MUNI_Cable = obs_NTD_df[obs_NTD_df["operator"] == "MUNI"]["cable_car_total"] / ratio[ratio["operator"] == "MUNI" ]["ratio"]
    MUNI = {
        "Operator": ["MUNI-Bus", "MUNI-Rail", "MUNI_Cable"],
        "Observed": [MUNI_Bus.mean(), MUNI_Rail.mean(), MUNI_Cable.mean()],
    }
    MUNI = pd.DataFrame(MUNI)

    SCVTA_bus = obs_NTD_df[obs_NTD_df["operator"] == "SCVTA"]["bus_total"] / ratio[ratio["operator"] == "SCVTA" ]["ratio"]
    SCVTA_rail = obs_NTD_df[obs_NTD_df["operator"] == "SCVTA"]["light_rail_total"] / ratio[ratio["operator"] == "SCVTA" ]["ratio"]
    SCVTA = {
        "Operator": ["SCVTA-Bus", "SCVTA-LRT"],
        "Observed": [SCVTA_bus.mean(), SCVTA_rail.mean()],
    }
    SCVTA = pd.DataFrame(SCVTA)

    # All observed data
    total = pd.concat(
        [obs_NTD_avgupt, df_AC, GG_Transit, MUNI, SCVTA], ignore_index=True
    )
    df_filtered = total.loc[~total["Operator"].isin(["MUNI", "GG Transit", "SCVTA"])]
    df_filtered = df_filtered.sort_values(by="Operator", ascending=True)
    return df_filtered


name_mapping = {
    "AC Transit": "AC-Transit",
    "Caltrain": "CalTrain",
    "FAST": "FAST",
    "LAVTA-Wheels": "LAVTA",
    "SF MUNI": "MUNI",
    "MUNI_Cable": "MUNI-Cable",
    "Golden Gate Transit": "GG Transit",
    "Napa Vine": "VINE",
    "Petaluma Transit": "Petaluma",
    "SF Bay Ferry": "SF Bay Ferry (WETA)",
    "Santa Rosa CityBus": "Santa Rosa",
    "Tri-Delta": "Tri Delta Transit",
    "Union City": "Union City Transit",
    "Vacaville City Coach": "Vacaville City Coach",
    "WestCAT": "WestCat",
    "Vacaville": "Vacaville City Coach",
    "Shuttle": "Standford Shuttles",
    "Muni-Bus": "MUNI-Bus",
    "Muni-rail": "MUNI-Rail",
    "Muni-Cable": "MUNI-Cable",
}

service_operator_dict = {
    "Premium": ["AC Transbay", "ACE", "Amtrak", "CalTrain", "GGT-Bus", "SMART"],
    "Local Bus": [
        "AC Eastbay",
        "CCCTA",
        "EmeryGoRound",
        "FAST",
        "LAVTA",
        "MUNI-Bus",
        "Petaluma",
        "Rio Vista Delta Breeze",
        "SamTrans",
        "Santa Rosa",
        "SCVTA-Bus",
        "SolTrans",
        "Sonoma County Transit",
        "Standford Shuttles",
        "Tri Delta Transit",
        "Union City Transit",
        "Vacaville City Coach",
        "VINE",
        "WestCat",
    ],
    "BART": ["BART"],
    "Light Rail": ["MUNI-Rail", "MUNI-Cable", "SCVTA-LRT"],
    "Ferry": ["GGT-Ferry", "SF Bay Ferry (WETA)"],
    None: ["AC-Transit"],
}


# Get Ferry data
def assign_ferry_name(name):
    transbay = [
        "90_FAOF",
        "90_FBHB",
        "90FOAF",
        "90_HBFB",
        "90_OAFW",
        "90_OAKOP",
        "90_OPOAK",
        "90_WFAO",
    ]
    GG_transit = [
        "91_LARKN",
        "91_LARKS",
        "92_FBSAU",
        "92_SAUFB",
        "93_FBTIB",
        "93_FWSAU",
        "93_FWTIB",
        "93_SAUFW",
        "93_TIBFB",
        "93_TIBFW",
    ]
    if name in transbay:
        return "SF Bay Ferry"
    elif name in GG_transit:
        return "GGT-Ferry"
    else:
        return "Other"


def assign_muni_name(name):
    cable = ["Cable Car"]
    rail = ["LRV1", "LRV2"]
    if name in cable:
        return "Cable Car"
    elif name in rail:
        return "Rail"
    else:
        return "Bus"


def process_total_val(combined_gdf):
    model_operator = read_dbf_and_groupby_sum(combined_gdf, None, ["SYSTEM"], "AB_BRDA")
    model_operator = model_operator.groupby("SYSTEM")["AB_BRDA"].sum().reset_index()
    model_operator = model_operator.rename(
        columns={"SYSTEM": "Operator", "AB_BRDA": "Modeled"}
    )

    all_mode = read_dbf_and_groupby_sum(combined_gdf, None, ["MODE"], "AB_BRDA")
    all_mode = all_mode.groupby("MODE")["AB_BRDA"].sum().reset_index()
    all_mode = all_mode.rename(columns={"MODE": "Operator", "AB_BRDA": "Modeled"})

    muni_df = read_dbf_and_groupby_sum(combined_gdf, "SF MUNI", "VEHTYPE", "AB_BRDA")
    muni_df["MUNI_name"] = muni_df["VEHTYPE"].apply(assign_ferry_name)
    muni = muni_df.groupby("MUNI_name")["AB_BRDA"].sum().reset_index()

    ferry_df = read_dbf_and_groupby_sum(
        combined_gdf, "Ferry", "NAME", "AB_BRDA"
    )  # List to collect DataFrames
    ferry_df["Ferry_name"] = ferry_df["NAME"].apply(assign_ferry_name)
    ferry = ferry_df.groupby("Ferry_name")["AB_BRDA"].sum().reset_index()

    ac = read_dbf_and_groupby_sum(combined_gdf, "AC Transit", "MODE", "AB_BRDA")
    vta = read_dbf_and_groupby_sum(combined_gdf, "SCVTA", "MODE", "AB_BRDA")

    GG_bus_model = all_mode[all_mode["Operator"] == 23]["Modeled"].mean()
    VTA_LR_model = vta[vta["MODE"] == 21]["AB_BRDA"].mean()
    VTA_bus_model = vta[vta["MODE"] == 19]["AB_BRDA"].mean()
    model_muni_bus = all_mode[all_mode["Operator"] == 11]["Modeled"].mean()
    model_muni_cable = all_mode[all_mode["Operator"] == 14]["Modeled"].mean()
    model_muni_rail = all_mode[all_mode["Operator"] == 15]["Modeled"].mean()
    ac_trans = ac[ac["MODE"] == 22]["AB_BRDA"].mean()
    ac_east = ac[ac["MODE"] == 18]["AB_BRDA"].mean()
    model_GG_ferry = ferry[ferry["Ferry_name"] == "GGT-Ferry"]["AB_BRDA"].mean()
    model_SF_Bay = ferry[ferry["Ferry_name"] == "SF Bay Ferry"]["AB_BRDA"].mean()

    model_dic = {
        "Operator": [
            "GGT-Ferry",
            "GGT-Bus",
            "SCVTA-Bus",
            "SCVTA-LRT",
            "SF Bay Ferry (WETA)",
            "Muni-Bus",
            "Muni-rail",
            "Muni-Cable",
            "AC Transbay",
            "AC Eastbay",
        ],
        "Modeled": [
            model_GG_ferry,
            GG_bus_model,
            VTA_bus_model,
            VTA_LR_model,
            model_SF_Bay,
            model_muni_bus,
            model_muni_rail,
            model_muni_cable,
            ac_trans,
            ac_east,
        ],
    }
    df_model_dic = pd.DataFrame(model_dic)
    df_modeled = pd.concat([model_operator, df_model_dic])
    df_sf_ferry = pd.DataFrame(
        {"Operator": ["SF Bay Ferry (WETA)"], "Modeled": [model_SF_Bay]}
    )
    model_operator = pd.concat([model_operator, df_sf_ferry])
    model_operator["Operator"] = (
        model_operator["Operator"].map(name_mapping).fillna(model_operator["Operator"])
    )
    return df_modeled, model_operator


def process_valTotal_operator(
    combined_gdf,
    transit_input_dir,
    markdown_output_dir,
    total_output_dir,
    observed_NTD,
    valTotal_Operator_md,
    valTotal_Operator,
):
    observal_operator = obs_ntd_table(transit_input_dir, observed_NTD)
    observal_operator["Operator"] = (
        observal_operator["Operator"]
        .map(name_mapping)
        .fillna(observal_operator["Operator"])
    )
    df_modeled, model_operator = process_total_val(combined_gdf)
    df_operator = pd.merge(observal_operator, model_operator, on="Operator", how="left")
    total_row_o = pd.Series(df_operator[["Observed", "Modeled"]].sum(), name="Total")
    df_operator = pd.concat([df_operator, total_row_o.to_frame().T], ignore_index=True)
    df_operator["Diff"] = df_operator["Modeled"] - df_operator["Observed"]
    df_operator["Pct Diff"] = (df_operator["Diff"] / df_operator["Observed"]) * 100
    df_operator.at[df_operator.index[-1], "Operator"] = "Total"
    total_operator = format_dataframe(
        df_operator, ["Observed", "Modeled", "Diff"], ["Pct Diff"]
    )
    dataframe_to_markdown(
        total_operator,
        file_name=markdown_output_dir / valTotal_Operator_md,
        highlight_rows=[23],
        center_align_columns=None,
        column_widths=100,
    )
    total_operator[:-1].to_csv(total_output_dir / valTotal_Operator, index=False)


def process_valTotal_Submode(
    combined_gdf,
    transit_input_dir,
    markdown_output_dir,
    total_output_dir,
    observed_NTD,
    valTotal_Submode,
    valTotal_Submode_md,
    valTotal_Service_md,
    valTotal_Service,
):
    df = pd.DataFrame(
        [
            (key, value)
            for key, values in service_operator_dict.items()
            for value in values
        ],
        columns=["Service Type", "Operator"],
    )

    df_filtered = calcualte_weekday_upt(transit_input_dir, observed_NTD)
    df_modeled, model_operator = process_total_val(combined_gdf)

    df_filtered["Operator"] = (
        df_filtered["Operator"].map(name_mapping).fillna(df_filtered["Operator"])
    )
    df_modeled["Operator"] = (
        df_modeled["Operator"].map(name_mapping).fillna(df_modeled["Operator"])
    )
    # Now perform the join, assuming you want to merge df1 and df2 based on the 'Operator' column
    result_df = pd.merge(df, df_filtered, on="Operator", how="left")
    result_df = pd.merge(result_df, df_modeled, on="Operator", how="left")
    result_df = result_df.sort_values(by="Operator")
    total_row = pd.Series(result_df[["Observed", "Modeled"]].sum(), name="Total")
    result_df = pd.concat([result_df, total_row.to_frame().T], ignore_index=True)
    result_df["Diff"] = result_df["Modeled"] - result_df["Observed"]
    result_df["Pct Diff"] = (result_df["Diff"] / result_df["Observed"]) * 100
    result_df.at[result_df.index[-1], "Operator"] = "Total"
    result_df = result_df[result_df["Operator"] != "AC-Transit"]

    result_csv = result_df.copy()
    result_csv = result_csv[:-1]
    result_csv = result_csv.dropna()
    result_csv.to_csv(total_output_dir / valTotal_Submode, index=False)

    total_val = format_dataframe(
        result_df, ["Observed", "Modeled", "Diff"], ["Pct Diff"]
    )
    dataframe_to_markdown(
        total_val,
        file_name=markdown_output_dir / valTotal_Submode_md,
        highlight_rows=[32],
        center_align_columns=["Operator"],
        column_widths=100,
    )

    df_service_type = (
        result_df[["Service Type", "Observed", "Modeled"]]
        .groupby("Service Type")
        .sum()
        .reset_index()
    )
    st_total_row = pd.Series(
        df_service_type[["Observed", "Modeled"]].sum(), name="Total"
    )
    df_service_type = pd.concat(
        [df_service_type, st_total_row.to_frame().T], ignore_index=True
    )
    df_service_type["Diff"] = df_service_type["Modeled"] - df_service_type["Observed"]
    df_service_type["Pct Diff"] = (
        df_service_type["Diff"] / df_service_type["Observed"]
    ) * 100
    df_service_type.at[df_service_type.index[-1], "Service Type"] = "Total"
    total_service = format_dataframe(
        df_service_type, ["Observed", "Modeled", "Diff"], ["Pct Diff"]
    )
    dataframe_to_markdown(
        total_service,
        file_name=markdown_output_dir / valTotal_Service_md,
        highlight_rows=[5],
        center_align_columns=None,
        column_widths=100,
    )
    total_service[:-1].to_csv(total_output_dir / valTotal_Service, index=False)


if __name__ == "__main__":
    with open("transit.toml", "rb") as f:
        config = tomllib.load(f)

    model_run_dir = config["directories"]["model_run"]
    transit_input_dir = config["directories"]["transit_input_dir"]
    markdown_output_dir = config["directories"]["markdown_output_dir"]
    total_output_dir = config["directories"]["total_output_dir"]
    observed_NTD = config["transit"]["observed_NTD"]
    valTotal_Submode_md = config["total"]["valTotal_Submode_md"]
    valTotal_Service_md = config["total"]["valTotal_Service_md"]
    valTotal_Operator_md = config["total"]["valTotal_Operator_md"]
    valTotal_Service = config["total"]["valTotal_Service"]
    valTotal_Operator = config["total"]["valTotal_Operator"]
    valTotal_Submode = config["total"]["valTotal_Submode"]
    output_dir = model_run_dir / "validation_workbook" / "output"
    output_transit_dir = output_dir / "transit"
    output_transit_dir.mkdir(parents=True, exist_ok=True)
    combined_gdf = read_transit_assignments(model_run_dir, time_periods)

    process_valTotal_operator(
        combined_gdf,
        transit_input_dir,
        markdown_output_dir,
        total_output_dir,
        observed_NTD,
        valTotal_Operator_md,
        valTotal_Operator,
    )
    process_valTotal_Submode(
        combined_gdf,
        transit_input_dir,
        markdown_output_dir,
        total_output_dir,
        observed_NTD,
        valTotal_Submode,
        valTotal_Submode_md,
        valTotal_Service_md,
        valTotal_Service,
    )

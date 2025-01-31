import pandas as pd
import toml
from transit.utils import (
    dataframe_to_markdown,
    format_dataframe,
    read_dbf_and_groupby_sum,
    read_transit_assignments,
    time_periods,
)


# Get Observed data from NTD
def obs_ntd_table(transit_input_dir, observed_NTD):
    obs_NTD_df = pd.read_csv(transit_input_dir / observed_NTD)
    obs_NTD_df["average weekday_upt"] = obs_NTD_df.apply(
        lambda row: round(row["annual_upt"] / 261)
        if pd.isna(row["average weekday_upt"])
        else row["average weekday_upt"],
        axis=1,
    )
    obs_NTD_df['ratio'] = obs_NTD_df['annual_upt'] / obs_NTD_df['average weekday_upt'].fillna(0)
    obs_NTD_df['ratio'] = obs_NTD_df['ratio'].where(obs_NTD_df['average weekday_upt'].notnull(), 0)
    # Identify columns to update using iloc
    columns_to_update_start = obs_NTD_df.columns.get_loc("bus_total")
    columns_to_update_end = obs_NTD_df.columns.get_loc("demand_response_total") + 1

    # Update the values in the selected columns using iloc
    obs_NTD_df.iloc[:, columns_to_update_start:columns_to_update_end] = obs_NTD_df.iloc[:, columns_to_update_start:columns_to_update_end].div(obs_NTD_df['ratio'], axis=0)
    obs_NTD_df["average weekday_upt"] = obs_NTD_df['average weekday_upt'] - obs_NTD_df["demand_response_total"]
    obs_NTD_avgupt = obs_NTD_df[["operator", "average weekday_upt"]]
    obs_NTD_avgupt = obs_NTD_avgupt.rename(
        columns={"operator": "Operator", "average weekday_upt": "Observed"}
    )
    return obs_NTD_avgupt, obs_NTD_df


def calcualte_weekday_upt(transit_input_dir, observed_NTD):
    obs_NTD_df = pd.read_csv(transit_input_dir / observed_NTD)
    obs_NTD_avgupt, obs_NTD_df = obs_ntd_table(transit_input_dir, observed_NTD)
    # ratio = obs_NTD_df[["operator", "annual_upt", "average weekday_upt"]]
    # ratio["ratio"] = ratio["annual_upt"]/ratio["average weekday_upt"].fillna(261)
    
    # Calculte AC Transit submode
    AC = {
        "Operator": ["AC Transbay", "AC Eastbay"],
        "Observed": [
            obs_NTD_df[obs_NTD_df["operator"] == "AC-Transit"]["commuter_bus_total"].iloc[0], 
            obs_NTD_df[obs_NTD_df["operator"] == "AC-Transit"]["bus_total"].iloc[0] + obs_NTD_df[obs_NTD_df["operator"] == "AC-Transit"]["bus_rapid_transit_total"].iloc[0]
        ],
    }

    df_AC = pd.DataFrame(AC)

    # Calculte Goden Gate Transit submode
    GG = {
        "Operator": ["GGT-Bus", "GGT-Ferry"],
        "Observed": [obs_NTD_df[obs_NTD_df["operator"] == "GG Transit"]["bus_total"].iloc[0],
                    obs_NTD_df[obs_NTD_df["operator"] == "GG Transit"]["ferry_total"].iloc[0]],
    }
    GG_Transit = pd.DataFrame(GG)
    
    # Calculte MUNI submode
    MUNI = {
        "Operator": ["MUNI-Bus", "MUNI-Rail", "MUNI-Cable", "MUNI-Streetcar"],
        "Observed": [obs_NTD_df[obs_NTD_df["operator"] == "MUNI"]["bus_total"].iloc[0] + obs_NTD_df[obs_NTD_df["operator"] == "MUNI"]["trolleybus_total"].iloc[0], 
                    obs_NTD_df[obs_NTD_df["operator"] == "MUNI"]["light_rail_total"].iloc[0],
                    obs_NTD_df[obs_NTD_df["operator"] == "MUNI"]["cable_car_total"].iloc[0],
                    obs_NTD_df[obs_NTD_df["operator"] == "MUNI"]["street_car_total"].iloc[0]],
    }
    MUNI = pd.DataFrame(MUNI)

    # Calculte SCVTA submode
    SCVTA = {
        "Operator": ["SCVTA-Bus", "SCVTA-LRT"],
        "Observed": [obs_NTD_df[obs_NTD_df["operator"] == "SCVTA"]["bus_total"].iloc[0],
                    obs_NTD_df[obs_NTD_df["operator"] == "SCVTA"]["light_rail_total"].iloc[0]],
    }
    SCVTA = pd.DataFrame(SCVTA)

    # All observed data
    total = pd.concat(
        [obs_NTD_avgupt, df_AC, GG_Transit, MUNI, SCVTA], ignore_index=True
    )
    df_filtered = total.loc[~total["Operator"].isin(["MUNI", "GG Transit", "SCVTA", "AC-Transit"])]
    df_filtered = df_filtered.sort_values(by="Operator", ascending=True)
    return df_filtered


name_mapping = {
    "AC Transit": "AC-Transit",
    "Caltrain": "Caltrain",
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
        "GGT-Other"
    ],
    "BART": ["BART"],
    "Light Rail": ["MUNI-Rail", "MUNI-Cable", "SCVTA-LRT", "MUNI-Streetcar"],
    "Ferry": ["GGT-Ferry", "SF Bay Ferry (WETA)", "Ferry Other"],
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


def process_total_val(combined_gdf, output_dir, model_MUNI_Line):

    all_mode = read_dbf_and_groupby_sum(combined_gdf, None, ["SYSTEM","MODE"], "AB_BRDA")
    all_mode = all_mode.groupby(["SYSTEM","MODE"])["AB_BRDA"].sum().reset_index()
    all_mode = all_mode.rename(columns={"MODE": "Operator", "AB_BRDA": "Modeled"})

    model_MUNI_line_df = pd.read_csv(output_dir / model_MUNI_Line)
    muni_mode_df = model_MUNI_line_df.groupby("Mode")['Ridership'].sum().reset_index()
    muni = {
        "Operator": ["MUNI-Bus", "MUNI-Rail", "MUNI-Cable", "MUNI-Streetcar"],
        "Modeled": [muni_mode_df[muni_mode_df["Mode"] == "Local Bus"]["Ridership"].iloc[0] 
                    + muni_mode_df[muni_mode_df["Mode"] == "Express Bus"]["Ridership"].iloc[0]
                    + muni_mode_df[muni_mode_df["Mode"] == "Rapid"]["Ridership"].iloc[0], 
                    muni_mode_df[muni_mode_df["Mode"] == "Rail"]["Ridership"].iloc[0],
                    muni_mode_df[muni_mode_df["Mode"] == "Cable Car"]["Ridership"].iloc[0],
                    muni_mode_df[muni_mode_df["Mode"] == "Streetcar"]["Ridership"].iloc[0]],
    }
    muni = pd.DataFrame(muni)

    ferry_df = read_dbf_and_groupby_sum(
        combined_gdf, "Ferry", "NAME", "AB_BRDA"
    )  # List to collect DataFrames
    ferry_df["Ferry_name"] = ferry_df["NAME"].apply(assign_ferry_name)
    ferry = ferry_df.groupby("Ferry_name")["AB_BRDA"].sum().reset_index()

    ac = read_dbf_and_groupby_sum(combined_gdf, "AC Transit", "MODE", "AB_BRDA")
    vta = read_dbf_and_groupby_sum(combined_gdf, "SCVTA", "MODE", "AB_BRDA")
    gg_transit = read_dbf_and_groupby_sum(combined_gdf, "Golden Gate Transit", "MODE", "AB_BRDA")

    GG_bus_model = gg_transit[gg_transit["MODE"] == 23]["AB_BRDA"].iloc[0]
    GG_local_model = gg_transit[gg_transit["MODE"] == 19]["AB_BRDA"].iloc[0]
    VTA_LR_model = vta[vta["MODE"] == 21]["AB_BRDA"].iloc[0]
    VTA_bus_model = vta[vta["MODE"] == 19]["AB_BRDA"].iloc[0] + vta[vta["MODE"] == 25]["AB_BRDA"].iloc[0] + vta[vta["MODE"] == 20]["AB_BRDA"].iloc[0]
    model_muni_bus = muni[muni["Operator"] == "MUNI-Bus"]["Modeled"].iloc[0]
    model_muni_cable = muni[muni["Operator"] == "MUNI-Cable"]["Modeled"].iloc[0]
    model_muni_rail = muni[muni["Operator"] == "MUNI-Rail"]["Modeled"].iloc[0]
    model_muni_street = muni[muni["Operator"] == "MUNI-Streetcar"]["Modeled"].iloc[0]
    ac_trans = ac[ac["MODE"] == 22]["AB_BRDA"].mean()
    ac_east = ac[ac["MODE"] == 18]["AB_BRDA"].mean()
    model_GG_ferry = ferry[ferry["Ferry_name"] == "GGT-Ferry"]["AB_BRDA"].mean()
    model_SF_Bay = ferry[ferry["Ferry_name"] == "SF Bay Ferry"]["AB_BRDA"].mean()
    model_other_ferry = ferry[ferry["Ferry_name"] == "Other"]["AB_BRDA"].mean()


    model_dic = {
        "Operator": [
            "GGT-Ferry",
            "GGT-Bus",
            "GGT-Other",
            "SCVTA-Bus",
            "SCVTA-LRT",
            "SF Bay Ferry (WETA)",
            "MUNI-Bus",
            "MUNI-Rail",
            "MUNI-Cable",
            "MUNI-Streetcar",
            "AC Transbay",
            "AC Eastbay",
            "Ferry Other"
        ],
        "Modeled": [
            model_GG_ferry,
            GG_bus_model,
            GG_local_model,
            VTA_bus_model,
            VTA_LR_model,
            model_SF_Bay,
            model_muni_bus,
            model_muni_rail,
            model_muni_cable,
            model_muni_street,
            ac_trans,
            ac_east,
            model_other_ferry
        ],
    }
    df_model_dic = pd.DataFrame(model_dic)
    model_operator = read_dbf_and_groupby_sum(combined_gdf, None, ["SYSTEM"], "AB_BRDA")
    model_operator = model_operator.groupby("SYSTEM")["AB_BRDA"].sum().reset_index()
    model_operator = model_operator.rename(
        columns={"SYSTEM": "Operator", "AB_BRDA": "Modeled"}
    )
    bart_update = model_operator[model_operator["Operator"].isin(["BART", "EBART", "OAC"])]["Modeled"].sum()

    # Update the BART row
    model_operator.loc[model_operator["Operator"] == "BART", "Modeled"] = bart_update
    model_operator = model_operator[~model_operator["Operator"].isin(["EBART", "OAC"])].reset_index(drop=True)
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
    output_dir,
    observed_NTD,
    valTotal_Operator_md,
    valTotal_Operator,
    model_MUNI_Line
):
    observal_operator, obs_NTD_df = obs_ntd_table(transit_input_dir, observed_NTD)
    observal_operator["Operator"] = (
        observal_operator["Operator"].map(name_mapping).fillna(observal_operator["Operator"])
    )
    df_modeled, model_operator = process_total_val(combined_gdf, output_dir, model_MUNI_Line)
    df_operator = pd.merge(observal_operator, model_operator, on="Operator", how="outer")
    modeled_other_sum = df_operator[df_operator["Observed"].isna()]["Modeled"].sum()

    # Add a new row for "Other"
    new_row = {
        "Operator": "Other",
        "Observed": None,
        "Modeled": modeled_other_sum,
    }
    df_operator = pd.concat([df_operator, pd.DataFrame([new_row])], ignore_index=True)
    # Ensure 'Other' row is included while dropping other rows with NaN in 'Observed'
    df_operator = df_operator[(df_operator["Observed"].notna()) | (df_operator["Operator"] == "Other")].reset_index(drop=True)

    modeled_total = df_operator[df_operator["Operator"] != "SF Bay Ferry (WETA)"]["Modeled"].sum()

    # Add a "Total" row excluding "SF Bay Ferry (WETA)"
    total_row = {
        "Operator": "Total",
        "Observed": df_operator["Observed"].sum(),
        "Modeled": modeled_total,
        "Diff": None,
        "Pct Diff": None
    }

    df_operator = pd.concat([df_operator, pd.DataFrame([total_row])], ignore_index=True)
    df_operator["Diff"] = df_operator["Modeled"] - df_operator["Observed"]
    df_operator["Pct Diff"] = (df_operator["Diff"] / df_operator["Observed"]) * 100
    df_operator.at[df_operator.index[-1], "Operator"] = "Total"
    total_operator = format_dataframe(
        df_operator, ["Observed", "Modeled", "Diff"], ["Pct Diff"]
    )
    dataframe_to_markdown(
        total_operator,
        file_name=output_dir / valTotal_Operator_md,
        highlight_rows=[24],
        center_align_columns=None,
        column_widths=100,
    )
    total_operator[:-1].to_csv(output_dir / valTotal_Operator, index=False)


def process_valTotal_Submode(
    combined_gdf,
    transit_input_dir,
    output_dir,
    observed_NTD,
    valTotal_Submode,
    valTotal_Submode_md,
    valTotal_Service_md,
    valTotal_Service,
    model_MUNI_Line
):

    df = pd.DataFrame(
        [
            (key, value)
            for key, values in service_operator_dict.items()
            for value in values
        ],
        columns=["Service Type", "Operator"],
    )

    mapping_data = {
        "Mode Number": [11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32],
        "Mode": [
            "Local Muni", "Express Muni", "BRT Muni", "Muni Cable Car", "LRT Muni", 
            "Free and Open Shuttles", "SamTrans Local", "AC Local", "Other Local MTC Buses", 
            "Regional BRT", "VTA LRT", "AC Transbay Buses", "Golden Gate Bus", "Sam Trans Express Bus", 
            "Other Premium Bus", "Caltrain", "SMART", "eBART", "Regional Rail/ACE/AMTRAK", 
            "HSR", "Ferry", "BART"
        ],
        "Service Type": [
            "Local Bus", "Local Bus", "Local Bus", "Light Rail", "Light Rail", 
            "Local Bus", "Local Bus", "Local Bus", "Local Bus", 
            "Local Bus", "Light Rail", "Premium", "Premium", "Premium", 
            "Premium", "Premium", "Premium", "BART", 
            "Premium", "Premium", "Ferry", "BART"
        ]
    }

    mapping_df = pd.DataFrame(mapping_data)
    
    all_mode = read_dbf_and_groupby_sum(combined_gdf, None, ["SYSTEM","MODE"], "AB_BRDA")
    all_mode = all_mode.groupby(["SYSTEM","MODE"])["AB_BRDA"].sum().reset_index()
    all_mode = all_mode.rename(columns={"MODE": "Operator", "AB_BRDA": "Modeled"})
    all_mode["Mode"] = all_mode["Operator"].map(mapping_df.set_index("Mode Number")["Mode"])
    all_mode["Service Type"] = all_mode["Operator"].map(mapping_df.set_index("Mode Number")["Service Type"])
    model_service_type = all_mode.groupby("Service Type")["Modeled"].sum().reset_index()
    df_filtered = calcualte_weekday_upt(transit_input_dir, observed_NTD)
    df_modeled, model_operator = process_total_val(combined_gdf, output_dir, model_MUNI_Line)
    df_filtered["Operator"] = (
        df_filtered["Operator"].map(name_mapping).fillna(df_filtered["Operator"])
    )
    df_modeled["Operator"] = (
        df_modeled["Operator"].map(name_mapping).fillna(df_modeled["Operator"])
    )
    df_modeled = df_modeled[~df_modeled["Operator"].isin(["MUNI","GG Transit","Ferry", "SCVTA","AC-Transit"])].reset_index(drop=True)
    result_df = pd.merge(df, df_filtered, on="Operator", how="outer")
    observed_service_type = result_df.groupby("Service Type")["Observed"].sum().reset_index()
    result_df = pd.merge(result_df, df_modeled, on="Operator", how="outer")
    result_other_sum = result_df[result_df["Observed"].isna()]["Modeled"].sum()

    # Add a new row for "Other"
    new_row = {
        "Operator": "Other",
        "Observed": None,
        "Modeled": result_other_sum,
    }
    result_df = pd.concat([result_df, pd.DataFrame([new_row])], ignore_index=True)
    result_df = result_df[(result_df["Observed"].notna()) | (result_df["Operator"] == "Other")].reset_index(drop=True)
    # result_df = result_df.sort_values(by="Operator")
    total_row = pd.Series(result_df[["Observed", "Modeled"]].sum(), name="Total")
    result_df = pd.concat([result_df, total_row.to_frame().T], ignore_index=True)
    result_df["Diff"] = result_df["Modeled"] - result_df["Observed"]
    result_df["Pct Diff"] = (result_df["Diff"] / result_df["Observed"]) * 100
    result_df.at[result_df.index[-1], "Operator"] = "Total"

    result_csv = result_df.copy()
    result_csv = result_csv[:-1]
    result_csv = result_csv.dropna()
    result_csv.to_csv(output_dir / valTotal_Submode, index=False)

    total_val = format_dataframe(
        result_df, ["Observed", "Modeled", "Diff"], ["Pct Diff"]
    )
    dataframe_to_markdown(
        total_val,
        file_name=output_dir / valTotal_Submode_md,
        highlight_rows=[30],
        center_align_columns=["Operator"],
        column_widths=100,
    )

    df_service_type = pd.merge(observed_service_type, model_service_type, on="Service Type", how="left")
    service_type_total_row = pd.Series(df_service_type[["Observed", "Modeled"]].sum(), name="Total")
    df_service_type = pd.concat([df_service_type, service_type_total_row.to_frame().T], ignore_index=True)
    df_service_type ["Diff"] = df_service_type ["Modeled"] - df_service_type ["Observed"]
    df_service_type ["Pct Diff"] = (df_service_type ["Diff"] / df_service_type["Observed"]) * 100
    df_service_type.loc[df_service_type.index[-1], "Service Type"] = "Total"
    total_service = format_dataframe(
        df_service_type, ["Observed", "Modeled", "Diff"], ["Pct Diff"]
    )
    dataframe_to_markdown(
        total_service,
        file_name=output_dir / valTotal_Service_md,
        highlight_rows=[5],
        center_align_columns=None,
        column_widths=100,
    )
    total_service[:-1].to_csv(output_dir / valTotal_Service, index=False)


if __name__ == "__main__":
    with open("transit.toml", "rb") as f:
        config = tomllib.load(f)

    process_valTotal_operator(
        combined_gdf,
        transit_input_dir,
        output_dir,
        observed_NTD,
        valTotal_Operator_md,
        valTotal_Operator,
        model_MUNI_Line
    )
    process_valTotal_Submode(
        combined_gdf,
        transit_input_dir,
        output_dir,
        observed_NTD,
        valTotal_Submode,
        valTotal_Submode_md,
        valTotal_Service_md,
        valTotal_Service,
        model_MUNI_Line
    )

import os
import geopandas as gpd
import tomli as tomllib
import pandas as pd
from transit_function import dataframe_to_markdown, format_dataframe, read_dbf_and_groupby_sum, read_transit_assignments

# Get Observed data from NTD
def obs_NTD_table(transit_input_dir, observed_NTD):
    
    # Read the Excel file, assuming headers are not in the first row
    df = pd.read_excel(
        transit_input_dir / observed_NTD, usecols="B:D", skiprows=list(range(4)), header=None, engine="openpyxl"
    )

    # A list to hold all the tables as DataFrames
    tables = []

    # A variable to keep track of the start of the current table
    current_table_start = None

    # Identifying header rows based on specific column content
    for index, row in df.iterrows():
        # Check if the row contains 'Annual UPT' and '% of total ridership', which indicates a header
        if "Annual UPT" in row.values and "% of total ridership" in row.values:
            # If we have a current table being built, add it to the tables list
            if current_table_start is not None:
                tables.append(df.iloc[current_table_start:index].reset_index(drop=True))
            # Set the start of the new table
            current_table_start = index

    if current_table_start is not None:
        tables.append(df.iloc[current_table_start:].reset_index(drop=True))

    for i, table in enumerate(tables):
        table.columns = table.iloc[0]
        table = table.drop(table.index[0])
        table = table.dropna(how="all", axis=0).reset_index(drop=True)
        # table['Annual UPT'] = table['Annual UPT'].apply(lambda x: format_numeric(x))
        table["% of total ridership"] = table["% of total ridership"].apply(
            lambda x: f"{x:.2%}" if not pd.isna(x) else x
        )
        table = table.reset_index(drop=True)
        tables[i] = table
        
    # Assuming 'tables' is a list of DataFrames
    new_df_data = []  # Prepare an empty list to store the data

    for df in tables:
        # Extract operator name - assuming it's the first cell of the first column in each table
        operator = df.columns[0]

        # Find 'Average Weekday UPT' if it exists, otherwise use 'Total Annual UPT'/260
        if "Average Weekday UPT" in df.iloc[:, 0].values:
            average_upt = df[df.iloc[:, 0] == "Average Weekday UPT"].iloc[0, 1]
        else:
            total_annual_upt = df[df.iloc[:, 0] == "Total Annual UPT"].iloc[0, 1]
            average_upt = (
                total_annual_upt / 260
            )  # Fallback if 'Average Weekday UPT' is not found

        # Append the result
        new_df_data.append([operator, average_upt])

    # Convert the list to a DataFrame
    new_df = pd.DataFrame(new_df_data, columns=["Operator", "Average Weekday UPT"])
    new_df = new_df.rename(columns={"Average Weekday UPT": "Observed"})
    return tables, new_df


def calcualte_weekday_UPT(transit_input_dir, observed_NTD):
    tables, obs_operator = obs_NTD_table(transit_input_dir, observed_NTD)
    AC_Transbay = tables[0][tables[0]["AC-Transit"] == "Commuter Bus"]["Annual UPT"] / 261
    AC_Eastbay = tables[0][tables[0]["AC-Transit"] == "Bus"]["Annual UPT"] / 261
    AC = {
        "Operator": ["AC Transbay", "AC Eastbay"],
        "Observed": [AC_Transbay.mean(), AC_Eastbay.mean()],
    }
    df_AC = pd.DataFrame(AC)

    GGT_bus = tables[6][tables[6]["GG Transit"] == "Bus"]["Annual UPT"] / 261
    GGT_Ferry = tables[6][tables[6]["GG Transit"] == "Ferryboat"]["Annual UPT"] / 261
    GG = {
        "Operator": ["GGT-Bus", "GGT-Ferry"],
        "Observed": [GGT_bus.mean(), GGT_Ferry.mean()],
    }
    GG_Transit = pd.DataFrame(GG)

    MUNI_Bus = tables[8][tables[8]["MUNI"] == "Bus"]["Annual UPT"] / 261
    MUNI_Rail = tables[8][tables[8]["MUNI"] == "Light Rail"]["Annual UPT"] / 261
    MUNI_Cable = tables[8][tables[8]["MUNI"] == "Cable Car"]["Annual UPT"] / 261
    MUNI = {
        "Operator": ["MUNI-Bus", "MUNI-Rail", "MUNI_Cable"],
        "Observed": [MUNI_Bus.mean(), MUNI_Rail.mean(), MUNI_Cable.mean()],
    }
    MUNI = pd.DataFrame(MUNI)

    SCVTA_bus = tables[14][tables[14]["SCVTA"] == "Bus"]["Annual UPT"] / 261
    SCVTA_rail = tables[14][tables[14]["SCVTA"] == "Light Rail"]["Annual UPT"] / 261
    SCVTA = {
        "Operator": ["SCVTA-Bus", "SCVTA-LRT"],
        "Observed": [SCVTA_bus.mean(), SCVTA_rail.mean()],
    }
    SCVTA = pd.DataFrame(SCVTA)

    # All observed data
    total = pd.concat([obs_operator, df_AC, GG_Transit, MUNI, SCVTA], ignore_index=True)
    df_filtered = total.loc[~total["Operator"].isin(["MUNI", "GG Transit", "SCVTA"])]
    df_filtered = df_filtered.sort_values(by="Operator", ascending=True)
    return df_filtered

name_mapping = {
    "AC Eastbay": "AC Eastbay",
    "AC Transbay": "AC Transbay",
    "AC-Transit": "AC-Transit",
    "ACE": "ACE",
    "BART": "BART",
    "CCCTA": "CCCTA",
    "CalTrain": "CalTrain",
    "FAST": "FAST",
    "GGT-Bus": "GGT-Bus",
    "GGT-Ferry": "GGT-Ferry",
    "LAVTA-Wheels": "LAVTA",
    "MUNI-Bus": "Muni - Bus",
    "MUNI-Rail": "Muni - Rail",
    "MUNI_Cable": "Muni - Cable",
    "Napa Vine": "VINE",
    "Petaluma Transit": "Petaluma",
    "Rio Vista Delta Breeze": "Rio Vista Delta Breeze ",
    "SCVTA-Bus": "SCVTA-Bus",
    "SCVTA-LRT": "SCVTA-LRT",
    "SF Bay Ferry": "SF Bay Ferry (WETA)",
    "SMART": "SMART",
    "SamTrans": "SamTrans",
    "Santa Rosa CityBus": "Santa Rosa",
    "SolTrans": "SolTrans",
    "Sonoma County Transit": "Sonoma County Transit",
    "Tri-Delta": "Tri Delta Transit",
    "Union City": "Union City Transit",
    "Vacaville City Coach": "Vacaville City Coach",
    "WestCat": "WestCat",
}

name_mapping2 = {
    "AC Eastbay": "AC Eastbay",
    "AC Transbay": "AC Transbay",
    "AC Transit": "AC-Transit",
    "ACE": "ACE",
    "BART": "BART",
    "CCCTA": "CCCTA",
    "Caltrain": "CalTrain",
    "FAST": "FAST",
    "GGT-Bus": "GGT-Bus",
    "GGT-Ferry": "GGT-Ferry",
    "LAVTA": "LAVTA",
    "Muni-Bus": "Muni - Bus",
    "Muni-rail": "Muni - Rail",
    "Muni-Cable": "Muni - Cable",
    "VINE": "VINE",
    "Petaluma": "Petaluma",
    "SCVTA-Bus": "SCVTA-Bus",
    "SCVTA-LRT": "SCVTA-LRT",
    "SF Bay Ferry (WETA)": "SF Bay Ferry (WETA)",
    "SMART": "SMART",
    "SamTrans": "SamTrans",
    "Santa Rosa CityBus": "Santa Rosa",
    "SolTrans": "SolTrans",
    "Shuttle": "Standford Shuttles",
    "Sonoma County Transit": "Sonoma County Transit",
    "Tri Delta Transit": "Tri Delta Transit",
    "Union City Transit": "Union City Transit",
    "Vacaville": "Vacaville City Coach",
    "WestCAT": "WestCat",
}

name_mapping3 = {
    "Golden Gate Transit": "GG Transit",
    "SF MUNI": "MUNI",
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


def process_total_val(dbf_file):
    
    model_operator = read_dbf_and_groupby_sum(dbf_file, None, ["SYSTEM"], "AB_BRDA")
    model_operator = model_operator.groupby("SYSTEM")["AB_BRDA"].sum().reset_index()
    model_operator = model_operator.rename(columns={"SYSTEM": "Operator", "AB_BRDA": "Modeled"})

    all_mode = read_dbf_and_groupby_sum(dbf_file, None, ["MODE"], "AB_BRDA")
    all_mode = all_mode.groupby("MODE")["AB_BRDA"].sum().reset_index()
    all_mode = all_mode.rename(columns={"MODE": "Operator", "AB_BRDA": "Modeled"})
    
    muni_df = read_dbf_and_groupby_sum(dbf_file, "SF MUNI","VEHTYPE", "AB_BRDA" )
    muni_df["MUNI_name"] = muni_df["VEHTYPE"].apply(assign_ferry_name)
    muni = muni_df.groupby("MUNI_name")["AB_BRDA"].sum().reset_index()

    
    ferry_df = read_dbf_and_groupby_sum(dbf_file,'Ferry', 'NAME', 'AB_BRDA')  # List to collect DataFrames
    ferry_df["Ferry_name"] = ferry_df["NAME"].apply(assign_ferry_name)
    ferry = ferry_df.groupby("Ferry_name")["AB_BRDA"].sum().reset_index()

    ac = read_dbf_and_groupby_sum(dbf_file, "AC Transit", "MODE", "AB_BRDA")
    
    GG_bus_model = all_mode[all_mode["Operator"] == 23]["Modeled"].mean()
    VTA_LR_model = all_mode[all_mode["Operator"] == 21]["Modeled"].mean()
    VTA_bus_model = all_mode[all_mode["Operator"] == 19]["Modeled"].mean()
    model_muni_bus = all_mode[all_mode["Operator"] == 11]["Modeled"].mean()
    model_muni_cable = muni[muni["MUNI_name"] == "Cable Car"]["AB_BRDA"].mean()
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
        {"Operator": ["SF Bay Ferry (WETA)"], "Modeled": [model_SF_Bay]})
    model_operator = pd.concat([model_operator, df_sf_ferry])
    model_operator["Operator"] = (
        model_operator["Operator"].map(name_mapping3).fillna(model_operator["Operator"])
    )
    return df_modeled, model_operator

def process_valTotal_Operator(dbf_file, transit_input_dir, markdown_output_dir, total_output_dir, observed_NTD, valTotal_Operator_md, valTotal_Operator):
    _, observal_operator = obs_NTD_table(transit_input_dir, observed_NTD)
    observal_operator["Operator"] = (
        observal_operator["Operator"]
        .map(name_mapping)
        .fillna(observal_operator["Operator"])
    )
    df_modeled, model_operator = process_total_val(dbf_file)
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
        file_name= markdown_output_dir / valTotal_Operator_md,
        highlight_rows=[23],
        center_align_columns=None,
        column_widths=100,
    )
    total_operator[:-1].to_csv(total_output_dir / valTotal_Operator, index=False)
    
def process_valTotal_Submode(transit_validation_2019_alfaro_filepath, dbf_file, transit_input_dir, markdown_output_dir, total_output_dir, observed_NTD, valTotal_Submode, valTotal_Submode_md, valTotal_Service_md, valTotal_Service):
    # TODO do NOT use "Transit_Validation_2019 - MA.xlsx" file for just line names etc
    # put the info/data into a CSV and put it into the resources dir or commit to repo
    df = pd.read_excel(
        transit_validation_2019_alfaro_filepath,
        usecols="C:D",
        sheet_name="val_Tot",
        skiprows=list(range(4)) + list(range(37, 51)),
    )
    
    df_filtered = calcualte_weekday_UPT(transit_input_dir, observed_NTD)
    df_modeled, model_operator = process_total_val(dbf_file)
    
    
    df_filtered["Operator"] = (
        df_filtered["Operator"].map(name_mapping).fillna(df_filtered["Operator"])
    )
    df_modeled["Operator"] = (
        df_modeled["Operator"].map(name_mapping2).fillna(df_modeled["Operator"])
    )
    # Now perform the join, assuming you want to merge df1 and df2 based on the 'Operator' column
    result_df = pd.merge(df, df_filtered, on="Operator", how="left")
    result_df = pd.merge(result_df, df_modeled, on="Operator", how="left")
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
    
    total_val = format_dataframe(result_df, ["Observed", "Modeled", "Diff"], ["Pct Diff"])
    dataframe_to_markdown(
        total_val,
        file_name= markdown_output_dir / valTotal_Submode_md,
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
    st_total_row = pd.Series(df_service_type[["Observed", "Modeled"]].sum(), name="Total")
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
        file_name= markdown_output_dir / valTotal_Service_md,
        highlight_rows=[5],
        center_align_columns=None,
        column_widths=100,
    )
    total_service[:-1].to_csv(total_output_dir / valTotal_Service, index=False)


if __name__ == "__main__":
    with open("transit.toml", "rb") as f:
        config = tomllib.load(f)

    transit_validation_2019_alfaro_filepath = config["transit"][
        "transit_validation_2019_alfaro_filepath"
    ]
    model_run_dir = config["directories"]["model_run"]
    transit_input_dir = config["directories"]["transit_input_dir"]
    markdown_output_dir = config["directories"]["markdown_output_dir"]
    total_output_dir = config["directories"]["total_output_dir"]
    observed_NTD = config["transit"]["observed_NTD"]
    valTotal_Submode_md = config["markdown"]["valTotal_Submode_md"]
    valTotal_Service_md = config["markdown"]["valTotal_Service_md"]
    valTotal_Operator_md = config["markdown"]["valTotal_Operator_md"]
    valTotal_Service = config["output"]["valTotal_Service"]
    valTotal_Operator = config["output"]["valTotal_Operator"]
    valTotal_Submode = config["output"]["valTotal_Submode"]
    output_dir = model_run_dir / "validation_workbook" / "output"
    output_transit_dir = output_dir / "transit"
    output_transit_dir.mkdir(parents=True, exist_ok=True)
    time_periods = ["EA", "AM", "MD", "PM", "EV"]
    dbf_file = read_transit_assignments(model_run_dir, time_periods)
    
    process_valTotal_Operator(dbf_file, transit_input_dir, markdown_output_dir, total_output_dir, observed_NTD, valTotal_Operator_md, valTotal_Operator)
    process_valTotal_Submode(transit_validation_2019_alfaro_filepath, dbf_file, transit_input_dir, markdown_output_dir, total_output_dir, observed_NTD, valTotal_Submode, valTotal_Submode_md, valTotal_Service_md, valTotal_Service)
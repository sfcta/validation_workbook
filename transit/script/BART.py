import os
import tomllib

import pandas as pd
import shapefile

with open("transit.toml", "rb") as f:
    config = tomllib.load(f)
WORKING_FOLDER = config["directories"]["transit_input_dir"]
OUTPUT_FOLDER = config["directories"]["transit_output_dir"]
AM_dbf = os.path.join(WORKING_FOLDER, config["transit"]["SFALLMSAAM_DBF"])
PM_dbf = os.path.join(WORKING_FOLDER, config["transit"]["SFALLMSAPM_DBF"])
MD_dbf = os.path.join(WORKING_FOLDER, config["transit"]["SFALLMSAMD_DBF"])
EV_dbf = os.path.join(WORKING_FOLDER, config["transit"]["SFALLMSAEV_DBF"])
EA_dbf = os.path.join(WORKING_FOLDER, config["transit"]["SFALLMSAEA_DBF"])
Nodes_File_Name = os.path.join(WORKING_FOLDER, config["transit"]["Nodes_File_Name"])
files_path = [AM_dbf, PM_dbf, MD_dbf, EV_dbf, EA_dbf]

file_create = [OUTPUT_FOLDER]
for path in file_create:
    if not os.path.exists(path):
        # If the folder does not exist, create it
        os.makedirs(path)
        print(f"Folder '{path}' did not exist and was created.")
    else:
        print(f"Folder '{path}' already exists.")

file_check = [WORKING_FOLDER, AM_dbf, PM_dbf, MD_dbf, EV_dbf, EA_dbf, Nodes_File_Name]
for path in file_check:
    if not os.path.exists(path):
        print(f"{path}: Not Exists")

node = pd.read_excel(Nodes_File_Name, header=None, skiprows=1)
node.columns = ["Node", "Node Name"]

station_name = {
    "Station": [
        "12TH",
        "16TH",
        "19TH",
        "24TH",
        "ANTC",
        "ASHB",
        "BALB",
        "BAYF",
        "CAST",
        "CIVC",
        "COLM",
        "COLS",
        "CONC",
        "DALY",
        "DBRK",
        "DELN",
        "DUBL",
        "EMBR",
        "FRMT",
        "FTVL",
        "GLEN",
        "HAYW",
        "LAFY",
        "LAKE",
        "MCAR",
        "MLBR",
        "MONT",
        "NBRK",
        "NCON",
        "OAKL",
        "ORIN",
        "PCTR",
        "PHIL",
        "PITT",
        "PLZA",
        "POWL",
        "RICH",
        "ROCK",
        "SANL",
        "SBRN",
        "SFIA",
        "SHAY",
        "SSAN",
        "UCTY",
        "WARM",
        "WCRK",
        "WDUB",
        "WOAK",
    ],
    "Node": [
        16509,
        16515,
        16508,
        16516,
        15231,
        16525,
        16518,
        16530,
        16537,
        16514,
        16539,
        16532,
        16501,
        16519,
        16523,
        16521,
        16538,
        16511,
        16526,
        16533,
        16517,
        16529,
        16504,
        16534,
        16507,
        16543,
        16512,
        16524,
        16535,
        16000,
        16505,
        15230,
        16502,
        16536,
        16522,
        16513,
        16520,
        16506,
        16531,
        16541,
        16542,
        16528,
        16540,
        16527,
        16544,
        16503,
        16545,
        16510,
    ],
}
df_station_name = pd.DataFrame(station_name)


def read_dbf_and_groupby_sum(dbf_file_path, system_filter, groupby_columns, sum_column):
    """
    Reads a DBF file, filters by system, groups by specified columns, and calculates sum of a specified column.

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


def process_BART_data(file_name, time, node, station):
    # Process BART data for different routes and columns
    BART_BRDA = read_dbf_and_groupby_sum(file_name, "BART", ["A"], "AB_BRDA")
    EBART_BRDA = read_dbf_and_groupby_sum(file_name, "EBART", ["A"], "AB_BRDA")
    OAC_BRDA = read_dbf_and_groupby_sum(file_name, "OAC", ["A"], "AB_BRDA")

    BART_XITA = read_dbf_and_groupby_sum(file_name, "BART", ["A"], "AB_XITA")
    EBART_XITA = read_dbf_and_groupby_sum(file_name, "EBART", ["A"], "AB_XITA")
    OAC_XITA = read_dbf_and_groupby_sum(file_name, "OAC", ["A"], "AB_XITA")

    # Concatenate and group data
    BART_A = pd.concat([BART_BRDA, EBART_BRDA, OAC_BRDA])
    BART_A = BART_A.groupby("A")["AB_BRDA"].sum().reset_index()
    BART_A.columns = ["Node", "AB_BRDA"]

    BART_B = pd.concat([BART_XITA, EBART_XITA, OAC_XITA])
    BART_B = BART_B.groupby("A")["AB_XITA"].sum().reset_index()
    BART_B.columns = ["Node", "AB_XITA"]

    # Merge with other dataframes
    BART_A = pd.merge(BART_A, node, on="Node", how="left")
    BART_A = pd.merge(BART_A, station, on="Node", how="right")
    BART = pd.merge(BART_A, BART_B, on="Node", how="right")

    # Drop rows with specific values
    values_to_drop = ["Hillcrest eBART", "Coliseium OAC", "Somersville Road eBART"]
    BART = BART[~BART["Node Name"].isin(values_to_drop)]

    # Add columns and rearrange columns
    BART["TOD"] = time
    BART["KEY"] = BART["Station"] + BART["TOD"]
    BART.columns = [
        "Node",
        "Boardings",
        "Node Name",
        "Station",
        "Alightings",
        "TOD",
        "Key",
    ]
    BART = BART[
        ["Node", "Node Name", "Station", "TOD", "Key", "Boardings", "Alightings"]
    ]

    # Sort and reset index
    BART = BART.sort_values(by="Station").reset_index(drop=True)

    return BART


data_frames = []  # List to collect DataFrames

for path in files_path:
    period = path[-6:-4]
    df = process_BART_data(path, period, node, df_station_name)
    df["TOD"] = period  # Add/ensure a 'TOD' column
    data_frames.append(df)

# Concatenate, select columns, sort, and reset index as before
BART = pd.concat(data_frames)
BART = BART[["Station", "TOD", "Key", "Boardings", "Alightings"]]
BART = BART.sort_values(by="Key").reset_index(drop=True)
BART.to_csv(os.path.join(OUTPUT_FOLDER, "model_BART.csv"), index=False)

BART_county = BART.copy()
counties = {
    "San Francisco": ["EMBR", "CIVC", "24TH", "MONT", "POWL", "GLEN", "16TH", "BALB"],
    "San Mateo": ["DALY", "COLM", "SSAN", "SBRN", "SFIA", "MLBR"],
    "Contra Costa": [
        "RICH",
        "ORIN",
        "LAFY",
        "WCRK",
        "CONC",
        "NCON",
        "PITT",
        "ANTC",
        "DELN",
        "PHIL",
        "PCTR",
        "PLZA",
    ],
    "Alameda": [
        "WOAK",
        "12TH",
        "19TH",
        "MCAR",
        "ASHB",
        "DUBL",
        "WDUB",
        "CAST",
        "WARM",
        "UCTY",
        "SHAY",
        "HAYW",
        "BAYF",
        "SANL",
        "OAKL",
        "COLS",
        "FTVL",
        "LAKE",
        "ROCK",
        "DBRK",
        "NBRK",
        "FRMT",
    ],
    "Santa Clara": [],  # Add stations for Santa Clara if available
}


# Function to map 'Station' to 'County'
def map_station_to_county(station):
    for county, stations in counties.items():
        if station in stations:
            return county
    return None  # Return None if the station is not found in any county


# Add the 'County' column to the DataFrame
BART_county["County"] = BART_county["Station"].apply(map_station_to_county)
BART_county = (
    BART_county.groupby(["County", "TOD"])[["Boardings", "Alightings"]]
    .sum()
    .reset_index()
)
BART_county.to_csv(os.path.join(OUTPUT_FOLDER, "model_BART_county.csv"), index=False)


# BART Screenline
def process_BART_SL_data(file_name, time, A, B):
    # Read the DBF file and group by 'A' and 'B' while summing 'AB_VOL'
    BART_SL = read_dbf_and_groupby_sum(file_name, "BART", ["A", "B"], "AB_VOL")

    # Filter rows for IB (16510 to 16511)
    IB = BART_SL[(BART_SL["A"] == A) & (BART_SL["B"] == B)].copy()
    IB["Direction"] = "IB"

    # Filter rows for OB (16511 to 16510)
    OB = BART_SL[(BART_SL["A"] == B) & (BART_SL["B"] == A)].copy()
    OB["Direction"] = "OB"

    # Concatenate IB and OB DataFrames
    result = pd.concat([IB, OB])

    # Add the 'TOD' column with the specified time
    result["TOD"] = time

    return result


def BART_SL_Concat(df, Screenline):
    # Concatenate the DataFrames
    BART_SL_CT = pd.concat(df)

    # Add the 'Screenline' column with 'Countyline'
    BART_SL_CT["Screenline"] = Screenline

    # Create the 'Key' column by combining 'Screenline', 'Direction', and 'TOD'
    BART_SL_CT["Key"] = (
        BART_SL_CT["Screenline"] + BART_SL_CT["Direction"] + BART_SL_CT["TOD"]
    )

    # Rename the 'AB_VOL' column to 'Ridership'
    BART_SL_CT = BART_SL_CT.rename(columns={"AB_VOL": "Ridership"})

    # Select and reorder columns
    BART_SL_CT = BART_SL_CT[["Screenline", "Direction", "TOD", "Key", "Ridership"]]

    # Sort by 'Key' and reset the index
    BART_SL_CT = BART_SL_CT.sort_values(by="Key").reset_index(drop=True)

    return BART_SL_CT


transbay_node = [16510, 16511]  # 16510 in, 16511 out

BART_sl_tb = []  # List to collect DataFrames

for path in files_path:
    period = path[-6:-4]
    df = process_BART_SL_data(path, period, transbay_node[0], transbay_node[1])
    df["TOD"] = period  # Add/ensure a 'TOD' column
    BART_sl_tb.append(df)

BART_SL_TB = BART_SL_Concat(BART_sl_tb, "Transbay")

countyline_node = [16519, 16518]  # 16519n-- in, 16518 --out

BART_sl_ct = []  # List to collect DataFrames

for path in files_path:
    period = path[-6:-4]
    df = process_BART_SL_data(path, period, countyline_node[0], countyline_node[1])
    df["TOD"] = period  # Add/ensure a 'TOD' column
    BART_sl_ct.append(df)

BART_SL_CT = BART_SL_Concat(BART_sl_ct, "Countyline")


station_locations = {
    "downtown": ["CIVC", "POWL", "MONT", "EMBR"],
    "not_downtown": ["GLEN", "BALB", "24TH", "16TH"],
}

# Create station to screenline mapping
station_to_label = {}
for loc in station_locations:
    stations = station_locations[loc]
    labels = [loc] * len(station_locations[loc])
    station_to_label = station_to_label | dict(zip(stations, labels))

relevant_stations = station_locations["downtown"] + station_locations["not_downtown"]


def determineSL(x):
    if (x["A_SL"] == "not_downtown") & (x["B_SL"] == "downtown"):
        return "IB"
    elif (x["A_SL"] == "downtown") & (x["B_SL"] == "not_downtown"):
        return "OB"
    else:
        return False


def process_SL_data(path, lines, node_df, df_station_name, relevant_stations, TOD):
    # Read, group, and sum data for each line
    dfs = [read_dbf_and_groupby_sum(path, line, ["A", "B"], "AB_VOL") for line in lines]

    # Concatenate results
    intra_am = pd.concat(dfs)

    # Mapping from Node and Station DataFrames
    node_mapping = node_df.set_index("Node")["Node Name"].to_dict()
    station_mapping = df_station_name.set_index("Node")["Station"].to_dict()

    # Apply mappings
    intra_am["A_name"] = intra_am["A"].map(node_mapping)
    intra_am["B_name"] = intra_am["B"].map(node_mapping)
    intra_am["A_station"] = intra_am["A"].map(station_mapping)
    intra_am["B_station"] = intra_am["B"].map(station_mapping)

    # Filter out specific values and stations
    values_to_drop = ["Hillcrest eBART", "Coliseium OAC", "Somersville Road eBART"]
    intra_am = intra_am[
        ~intra_am["A_name"].isin(values_to_drop)
        & ~intra_am["B_name"].isin(values_to_drop)
    ]
    intra_am = intra_am[
        intra_am["A_station"].isin(relevant_stations)
        & intra_am["B_station"].isin(relevant_stations)
    ]

    # Apply custom station labeling and direction determination
    intra_am["A_SL"] = intra_am["A_station"].apply(lambda x: station_to_label[x])
    intra_am["B_SL"] = intra_am["B_station"].apply(lambda x: station_to_label[x])
    intra_am["Direction"] = intra_am.apply(lambda x: determineSL(x), axis=1)
    intra_am = intra_am[intra_am["Direction"] != False]

    # Set time of day and select final columns
    intra_am["TOD"] = TOD
    intra_am = intra_am[["Direction", "TOD", "AB_VOL"]]

    return intra_am


lines = ["BART", "EBART", "OAC"]

BART_sl_sf = []  # List to collect DataFrames

for path in files_path:
    period = path[-6:-4]
    df = process_SL_data(path, lines, node, df_station_name, relevant_stations, period)
    BART_sl_sf.append(df)

intra_sf = pd.concat(BART_sl_sf)
intra_sf["Screenline"] = "Intra-SF"
intra_sf["Key"] = intra_sf["Screenline"] + intra_sf["Direction"] + intra_sf["TOD"]
intra_sf.columns = ["Direction", "TOD", "Ridership", "Screenline", "Key"]
intra_sf = intra_sf[["Screenline", "Direction", "TOD", "Key", "Ridership"]]
intra_sf = intra_sf.sort_values(by=["Direction", "TOD"]).reset_index(drop=True)

BART_SL = pd.concat([BART_SL_TB, BART_SL_CT, intra_sf], ignore_index=True)
BART_SL.to_csv(os.path.join(OUTPUT_FOLDER, "model_BART_SL.csv"), index=False)

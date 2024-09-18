from pathlib import Path
import pandas as pd
import tomllib
from transit_function import read_dbf_and_groupby_sum, read_transit_assignments

def read_nodes(model_run_dir):
    filepath = Path(model_run_dir) / "nodes.xls"
    return pd.read_excel(
        filepath, header=None, names=["Node", "Node Name"]
    )

def station_name():
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
    return df_station_name

def process_BART_data(file_name, model_run_dir, output_dir, model_BART):
    # Process BART data for different routes and columns
    nodes = read_nodes(model_run_dir)
    station = station_name()
    BART_BRDA = read_dbf_and_groupby_sum(file_name, "BART", ["A","TOD"], "AB_BRDA")
    EBART_BRDA = read_dbf_and_groupby_sum(file_name, "EBART", ["A", "TOD"], "AB_BRDA")
    OAC_BRDA = read_dbf_and_groupby_sum(file_name, "OAC", ["A", "TOD"], "AB_BRDA")

    BART_XITA = read_dbf_and_groupby_sum(file_name, "BART", ["A", "TOD"], "AB_XITA")
    EBART_XITA = read_dbf_and_groupby_sum(file_name, "EBART", ["A", "TOD"], "AB_XITA")
    OAC_XITA = read_dbf_and_groupby_sum(file_name, "OAC", ["A", "TOD"], "AB_XITA")

    # Concatenate and group data
    BART_A = pd.concat([BART_BRDA, EBART_BRDA, OAC_BRDA])
    BART_A = BART_A.groupby(["A", "TOD"])["AB_BRDA"].sum().reset_index()
    BART_A.columns = ["Node", "TOD", "AB_BRDA"]

    BART_B = pd.concat([BART_XITA, EBART_XITA, OAC_XITA])
    BART_B = BART_B.groupby(["A", "TOD"])["AB_XITA"].sum().reset_index()
    BART_B.columns = ["Node", "TOD", "AB_XITA"]

    # Merge with other dataframes
    BART_A = pd.merge(BART_A, nodes, on=["Node"], how="left")
    BART_A = pd.merge(BART_A, station, on=["Node"], how="right")
    BART = pd.merge(BART_A, BART_B, on=["Node", "TOD"], how="right")

    # Drop rows with specific values
    values_to_drop = ["Hillcrest eBART", "Coliseium OAC", "Somersville Road eBART"]
    BART = BART[~BART["Node Name"].isin(values_to_drop)]

    # Add columns and rearrange columns
    BART["Key"] = BART["Station"] + BART["TOD"]
    BART.columns = [
        "Node",
        "TOD",
        "Boardings",
        "Node Name",
        "Station",
        "Alightings",
        "Key",
    ]
    BART = BART[
        ["Node", "Node Name", "Station", "TOD", "Key", "Boardings", "Alightings"]
    ]

    # Sort and reset index
    BART = BART[["Station", "TOD", "Key", "Boardings", "Alightings"]]
    BART = BART.sort_values(by="Key").reset_index(drop=True)
    BART.to_csv(output_dir / model_BART, index=False)
    return BART

# Function to map 'Station' to 'County'
def map_station_to_county(station):
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
    for county, stations in counties.items():
        if station in stations:
            return county
    return None 

def process_BART_county(file_name, model_run_dir, output_dir, model_BART_county, model_BART):
    BART_county = process_BART_data(file_name, model_run_dir, output_dir, model_BART )
    
    # Add the 'County' column to the DataFrame
    BART_county["County"] = BART_county["Station"].apply(lambda x: map_station_to_county(x))
    BART_county = (
        BART_county.groupby(["County", "TOD"])[["Boardings", "Alightings"]]
        .sum()
        .reset_index()
    )
    BART_county.to_csv(output_dir / model_BART_county, index=False)


# BART Screenline
def process_BART_SL_data(file_name, A, B):
    # Read the DBF file and group by 'A' and 'B' while summing 'AB_VOL'
    BART_SL = read_dbf_and_groupby_sum(file_name, "BART", ["A", "B", "TOD"], "AB_VOL")

    # Filter rows for IB (16510 to 16511)
    IB = BART_SL[(BART_SL["A"] == A) & (BART_SL["B"] == B)].copy()
    IB["Direction"] = "IB"

    # Filter rows for OB (16511 to 16510)
    OB = BART_SL[(BART_SL["A"] == B) & (BART_SL["B"] == A)].copy()
    OB["Direction"] = "OB"

    # Concatenate IB and OB DataFrames
    result = pd.concat([IB, OB])

    return result


def BART_SL_Concat(file_name, A, B, Screenline):
    # Concatenate the DataFrames
    BART_SL = process_BART_SL_data(file_name, A, B)

    # Add the 'Screenline' column with 'Countyline'
    BART_SL["Screenline"] = Screenline

    # Create the 'Key' column by combining 'Screenline', 'Direction', and 'TOD'
    BART_SL["Key"] = (
        BART_SL["Screenline"] + BART_SL["Direction"] + BART_SL["TOD"]
    )

    # Rename the 'AB_VOL' column to 'Ridership'
    BART_SL = BART_SL.rename(columns={"AB_VOL": "Ridership"})

    # Select and reorder columns
    BART_SL = BART_SL[["Screenline", "Direction", "TOD", "Key", "Ridership"]]

    # Sort by 'Key' and reset the index
    BART_SL = BART_SL.sort_values(by="Key").reset_index(drop=True)

    return BART_SL

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


def process_BART_SF(filename, model_run_dir):
    nodes = read_nodes(model_run_dir)
    df_station_name = station_name()
    lines = ["BART", "EBART", "OAC"]
    
    # Read, group, and sum data for each line
    dfs = [read_dbf_and_groupby_sum(filename, line, ["A", "B", "TOD"], "AB_VOL") for line in lines]

    # Concatenate results
    intra = pd.concat(dfs)

    # Mapping from Node and Station DataFrames
    node_mapping = nodes.set_index("Node")["Node Name"].to_dict()
    station_mapping = df_station_name.set_index("Node")["Station"].to_dict()

    # Apply mappings
    intra["A_name"] = intra["A"].map(node_mapping)
    intra["B_name"] = intra["B"].map(node_mapping)
    intra["A_station"] = intra["A"].map(station_mapping)
    intra["B_station"] = intra["B"].map(station_mapping)

    # Filter out specific values and stations
    values_to_drop = ["Hillcrest eBART", "Coliseium OAC", "Somersville Road eBART"]
    intra = intra[
        ~intra["A_name"].isin(values_to_drop)
        & ~intra["B_name"].isin(values_to_drop)
    ]
    intra = intra[
        intra["A_station"].isin(relevant_stations)
        & intra["B_station"].isin(relevant_stations)
    ]

    # Apply custom station labeling and direction determination
    intra["A_SL"] = intra["A_station"].apply(lambda x: station_to_label[x])
    intra["B_SL"] = intra["B_station"].apply(lambda x: station_to_label[x])
    intra["Direction"] = intra.apply(lambda x: determineSL(x), axis=1)
    intra = intra[intra["Direction"] != False]

    # Set time of day and select final columns
    intra = intra[["Direction", "TOD", "AB_VOL"]]
    # 2019 validation uses 'Intra-SF'
    intra['Screenline'] = 'SF-San Mateo'
    intra['Key'] = intra['Screenline'] + intra['Direction'] + intra['TOD']
    intra.columns = ['Direction', 'TOD', 'Ridership', 'Screenline', 'Key']
    intra = intra[['Screenline', 'Direction', 'TOD', 'Key', 'Ridership']]
    intra = intra.sort_values(by=['Direction','TOD'] ).reset_index(drop=True)
    
    return intra

def process_BART_SL(file_name, model_run_dir, output_dir, model_BART_SL):
    transbay_node = [16510, 16511]  # 16510 in, 16511 out
    BART_sl_tb = BART_SL_Concat(file_name, transbay_node[0], transbay_node[1], 'Transbay')
    countyline_node = [16519, 16518]  # 16519n-- in, 16518 --out
    BART_sl_ct = BART_SL_Concat(file_name, countyline_node[0], countyline_node[1], "Countyline")
    BART_sf = process_BART_SF(file_name, model_run_dir)
    BART_SL = pd.concat([BART_sl_tb, BART_sl_ct, BART_sf], ignore_index=True)
    BART_SL.to_csv(output_dir / model_BART_SL, index=False)
    
if __name__ == "__main__":
    with open("transit.toml", "rb") as f:
        config = tomllib.load(f)
        
    model_run_dir = config["directories"]["model_run"]
    model_BART = config["output"]["model_BART"]
    model_BART_county = config["output"]["model_BART_county"]
    model_BART_SL = config["output"]["model_BART_SL"]
    output_dir = model_run_dir / "validation_workbook" / "output"
    output_transit_dir = output_dir / "transit"
    output_transit_dir.mkdir(parents=True, exist_ok=True)
    time_periods = ["EA", "AM", "MD", "PM", "EV"]
    dbf_file = read_transit_assignments(model_run_dir, time_periods)
    
    process_BART_SL(dbf_file, model_run_dir, output_dir, model_BART_SL)
    process_BART_county(dbf_file, model_run_dir, output_dir, model_BART_county)
    process_BART_data(dbf_file, model_run_dir, output_dir, model_BART)

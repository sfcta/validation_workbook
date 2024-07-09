import shapefile
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString
import numpy as np
from os import listdir
from os.path import isfile, join
import os
import tomllib

#MUNI
with open("transit.toml", "rb") as f:
    config = tomllib.load(f)
WORKING_FOLDER          =  config['directories']['transit_input_dir']
OUTPUT_FOLDER           =  config['directories']['transit_output_dir']
MUNI_output_dir         =  config['directories']['MUNI_output_dir']
BART_output_dir         =  config['directories']['BART_output_dir']
Base_model_dir          =  config['directories']['Base_model_dir']
SHP_file_dir            =  config['directories']['SHP_file_dir']
AM_dbf                  =  os.path.join(WORKING_FOLDER, config['transit']['SFALLMSAAM_DBF'])
PM_dbf                  =  os.path.join(WORKING_FOLDER, config['transit']['SFALLMSAPM_DBF'])
MD_dbf                  =  os.path.join(WORKING_FOLDER, config['transit']['SFALLMSAMD_DBF'])
EV_dbf                  =  os.path.join(WORKING_FOLDER, config['transit']['SFALLMSAEV_DBF'])
EA_dbf                  =  os.path.join(WORKING_FOLDER, config['transit']['SFALLMSAEA_DBF'])
observed_BART           =  os.path.join(WORKING_FOLDER, config['transit']['observed_BART'])
FREEFLOW_SHP            =  os.path.join(Base_model_dir, config['transit']['FREEFLOW_SHP'])
files_path = [AM_dbf, PM_dbf, MD_dbf, EV_dbf, EA_dbf]

file_create = [ OUTPUT_FOLDER, MUNI_output_dir, BART_output_dir, SHP_file_dir]
for path in file_create:
    if not os.path.exists(path):
        # If the folder does not exist, create it
        os.makedirs(path)
        print(f"Folder '{path}' did not exist and was created.")
    else:
        print(f"Folder '{path}' already exists.")

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
    filtered_df = df[df['SYSTEM'] == system_filter]
    
    # Group by the specified columns and sum the specified column
    grouped_sum = filtered_df.groupby(groupby_columns)[sum_column].sum()
    
    # Resetting index to convert it back to a DataFrame
    grouped_sum_df = grouped_sum.reset_index()
    
    return grouped_sum_df

def sort_dataframe_by_mixed_column(df, column_name):
    """
    Sorts a DataFrame by a column that contains mixed numeric-text values. Numeric prefixes are sorted numerically,
    and text entries are sorted alphabetically. Null values are sorted to the bottom.

    Parameters:
    df (DataFrame): The DataFrame to sort.
    column_name (str): The name of the column with mixed numeric-text values to sort by.

    Returns:
    DataFrame: The sorted DataFrame.
    """
    # Extract the numeric part and the text part into separate columns
    split_columns = df[column_name].str.extract(r'(?:(\d+) - )?(.*)')
    
    # Convert the numeric part to integers, using a placeholder for sorting null values
    placeholder_for_sorting = 999999  # Use a large number to sort nulls last
    split_columns[0] = pd.to_numeric(split_columns[0], errors='coerce').fillna(placeholder_for_sorting).astype(int)
    
    # Sort by the numeric part and then by the text part, ensuring null values go to the bottom
    df_sorted = df.loc[split_columns.sort_values(by=[0, 1], na_position='last').index]
    
    # Reset the index of the sorted DataFrame
    df_sorted = df_sorted.reset_index(drop=True)
    
    return df_sorted

def split_dataframe_by_name_ending(df, column_name):
    """
    Splits a DataFrame into two based on the last character of a specified column's values.
    
    Parameters:
    df (DataFrame): The DataFrame to split.
    column_name (str): The name of the column to check for the ending character.
    
    Returns:
    tuple: A tuple containing two DataFrames. The first with names ending in 'I', the second with names ending in 'O'.
    """
    # Filter the DataFrame for entries ending with 'I'
    df_ending_i = df[df[column_name].str.endswith('I')]
    
    # Filter the DataFrame for entries ending with 'O'
    df_ending_o = df[df[column_name].str.endswith('O')]
    
    return df_ending_i, df_ending_o

def map_name_to_direction(name):
    if name.endswith('I'):
        return 'IB'
    elif name.endswith('O'):
        return 'OB'
    else:
        return None  # Return None for other cases
    
def format_dataframe(df, numeric_columns, percentage_columns=None):
    """
    Format a DataFrame for readable display.
    - Fills NA values with '-'.
    - Formats specified numeric columns with commas and no decimal places.
    - Formats specified columns as percentages.
    
    Parameters:
    df (pd.DataFrame): The DataFrame to format.
    numeric_columns (list): List of numeric column names to format.
    percentage_columns (list): List of column names to format as percentages.

    Returns:
    pd.DataFrame: The formatted DataFrame.
    """
    if percentage_columns is None:
        percentage_columns = []

    # Fill NA values
    formatted_df = df.fillna('-')

    # Format specified numeric columns
    for col in numeric_columns:
        formatted_df[col] = formatted_df[col].apply(lambda x: format_numeric(x))

    # Format percentage columns
    for col in percentage_columns:
        formatted_df[col] = formatted_df[col].apply(lambda x: format_percentage(x))

    return formatted_df

def format_numeric(x):
    """ Format a numeric value with commas and no decimal places. """
    try:
        return f"{float(x):,.0f}" if x not in ['-', ''] else x
    except ValueError:
        return x

def format_percentage(x):
    """ Format a value as a percentage. """
    try:
        return f"{float(x):.0f}%" if x not in ['-', ''] else x
    except ValueError:
        return x
    
# Function to concatenate ordered geometries
def concat_ordered_geometries(group):
    sorted_geoms = group.sort_values(by='SEQ')['geometry']
    return LineString([pt for geom in sorted_geoms for pt in geom.coords])

# Convert station abbreviation to full name
abbr_to_full = {
    '12TH' : 'Oakland City Center',
    '16TH' : '16th/Mission',
    '19TH' : '19th St Oakland',
    '24TH' : '24th/Mission',
    'ANTC' : 'ANTC',                    # NOT IN MODEL (Antioch)
    'ASHB' : 'Ashby',
    'BALB' : 'Balboa Park',
    'BAYF' : 'Bay Fair',
    'BERY' : 'Berryessa',
    'CAST' : 'Castro Valley',
    'CIVC' : 'Civic Center',
    'COLM' : 'Colma',
    'COLS' : 'Coliseum OAK',
    'CONC' : 'Concord',
    'DALY' : 'Daly City',
    'DBRK' : 'Downtown Berkeley',
    'DELN' : 'El Cerrito del Norte',
    'DUBL' : 'Dublin/Pleasanton',
    'EMBR' : 'Embarcadero',
    'FRMT' : 'Fremont',
    'FTVL' : 'Fruitvale',
    'GLEN' : 'Glen Park',
    'HAYW' : 'Hayward',
    'LAFY' : 'Lafayette',
    'LAKE' : 'Lake Merritt',
    'MCAR' : 'MacArthur',
    'MLBR' : 'Millbrae',
    'MONT' : 'Montgomery',
    'MLPT' : 'Milpitas',
    'NBRK' : 'North Berkeley',
    'NCON' : 'North Concord',
    'OAKL' : 'OAKL',                   # NOT IN MODEL (Oakland International Airport)
    'ORIN' : 'Orinda',
    'PCTR' : 'PCTR',                   # NOT IN MODEL (Pittsburg Center Station)
    'PHIL' : 'Pleasant Hill',
    'PITT' : 'Pittsburg/Bay Point',
    'PLZA' : 'El Cerrito Plaza',
    'POWL' : 'Powell',
    'RICH' : 'Richmond',
    'ROCK' : 'Rockridge',
    'SANL' : 'San Leandro',
    'SBRN' : 'San Bruno',
    'SFIA' : 'SFO',
    'SHAY' : 'S Hayward',
    'SSAN' : 'South SF',
    'UCTY' : 'Union City',
    'WARM' : 'Warm Springs',
    'WCRK' : 'Walnut Creek',
    'WDUB' : 'West Dublin',
    'WOAK' : 'W Oakland'
}

station_name = {'Station': ['12TH', '16TH', '19TH', '24TH', 'ANTC', 'ASHB', 'BALB', 'BAYF', 'CAST', 'CIVC', 'COLM', 'COLS', 
                         'CONC', 'DALY', 'DBRK', 'DELN', 'DUBL', 'EMBR', 'FRMT', 'FTVL', 'GLEN', 'HAYW', 'LAFY', 'LAKE', 
                         'MCAR', 'MLBR', 'MONT', 'NBRK', 'NCON', 'OAKL', 'ORIN', 'PCTR', 'PHIL', 'PITT', 'PLZA', 'POWL', 
                         'RICH', 'ROCK', 'SANL', 'SBRN', 'SFIA', 'SHAY', 'SSAN', 'UCTY', 'WARM', 'WCRK', 'WDUB', 'WOAK', 
                         'MLPT', 'BERY'],
                'Node': [16509, 16515, 16508, 16516, 15231, 16525, 16518, 16530, 16537, 16514, 16539, 16532,
                         16501, 16519, 16523, 16521, 16538, 16511, 16526, 16533, 16517, 16529, 16504, 16534,
                         16507, 16543, 16512, 16524, 16535, 16000, 16505, 15230, 16502, 16536, 16522, 16513,
                         16520, 16506, 16531, 16541, 16542, 16528, 16540, 16527, 16544, 16503, 16545 , 16510, 
                         11093, 15203]}
df_station_name = pd.DataFrame(station_name)
df_station_name['Station_name'] = df_station_name['Station'].apply(lambda x : abbr_to_full[x])

station_name = {
    'Station': ['12TH', '16TH', '19TH', '24TH', 'EMBR', 'MONT', 'POWL', 'CIVC', 'GLEN', 'BALB', 'DALY', 'ANTC',
                'ASHB', 'UCTY', 'BAYF', 'CAST', 'COLM', 'COLS', 'CONC', 'DBRK', 'DELN', 'DUBL', 'FRMT', 'FTVL',
                'HAYW', 'LAFY', 'LAKE', 'MCAR', 'MLBR', 'NBRK', 'NCON', 'OAKL', 'ORIN', 'PCTR', 'PHIL', 'PITT',
                'PLZA', 'RICH', 'ROCK', 'SANL', 'SBRN', 'SFIA', 'SHAY', 'SSAN', 'WARM', 'WCRK', 'WDUB', 'WOAK', 
                'MLPT', 'BERY' ],
    # Find geometry in X:\Projects\ConnectSF\2050_Baseline
    'geometry': [
        (6049767.14, 2119506.7165), (6006767.6834, 2106509.6581), (6050867.2736, 2121607.9255),
        (6007065.5285, 2101807.0389), (6013668.588, 2116709.2907), (6012268.7237, 2115308.7612),
        (6010767.7227, 2113809.1607), (6008766.21, 2112007.7684), (6002267.4332, 2095109.9198),
        (5998566.8683, 2090908.0746), (5992068.011, 2085310.1909),(6190907.33, 2188926.7452),
        (6050568.4709, 2138306.2913),(6122066.435, 2040507.4321), (6090665.2322, 2081908.2387),
        (6105865.8549, 2078508.3059), (5992566.8931,2077808.348), (6071765.9655, 2101207.5399),
        (6120867.9426, 2181006.9044), (6051067.9389, 2144707.7734), (6037763.8869, 2164902.414),
        (6157366.8859, 2081006.495), (6133866.765, 2027908.4692), (6063066.6973, 2109207.3256), 
        (6102965.7997, 2071205.9733), (6092368.2914, 2152007.3679), (6051067.7192, 2117807.513),
        (6051867.9655, 2128708.394), (6015665.034, 2045809.4521), (6046968.3673, 2145106.2995), 
        (6122569.3244, 2191706.0269), (6061416.3918, 2091921.0042), (6075866.7507, 2146907.2611), 
        (6161632.1229, 2195698.4677), (6112666.5933, 2163804.4841), (6146166.9892, 2195906.2658), 
        (6042168.6724, 2155907.4969), (6027067.3566, 2168806.3649), (6055968.0873, 2135207.2884), 
        (6081167.6265, 2090007.967), (6006665.742, 2060908.5058), (6014967.7221, 2051908.139), 
        (6111466.3558, 2057608.1106), (5999066.9282, 2069808.0747), (6142865.7593, 2009107.7673),
        (6109469.0872, 2155705.9545), (6148868.323, 2080406.2284), (6043866.7193, 2120206.1134),
        (6156866.4816, 1974108.2663), (6162684.7021, 1961206.4605)
    ]
}


# Convert the geometry list of tuples into a list of Shapely Point objects
station_name['geometry'] = [Point(xy) for xy in station_name['geometry']]
# Create a GeoDataFrame
station = gpd.GeoDataFrame(station_name, geometry='geometry')
station = station.merge(df_station_name,  on='Station', how='left')


#MUNI
MUNI = []
for path in files_path:
    df = read_dbf_and_groupby_sum(path, "SF MUNI", ['FULLNAME', 'NAME','AB','SEQ'], 'AB_BRDA')
    df = sort_dataframe_by_mixed_column(df, 'FULLNAME')
    df ['Direction'] = df ['NAME'].apply(map_name_to_direction)
    period = path[-6:-4]
    df['TOD'] = period
    MUNI.append(df)
    
MUNI_day = pd.concat(MUNI)
MUNI_day = MUNI_day.groupby(['FULLNAME','NAME','AB', 'SEQ', 'Direction'], as_index=False)['AB_BRDA'].sum()
MUNI_day = MUNI_day.rename(columns={"NAME": "Name"})
model_MUNI_line = pd.read_csv(os.path.join(OUTPUT_FOLDER,'model_MUNI_line.csv'))
MUNI_map = model_MUNI_line.merge(MUNI_day, on='Name', how='left')
MUNI_map = MUNI_map[['Name','Line','AB', 'SEQ']]
MUNI_map ['Direction'] = MUNI_map ['Name'].apply(map_name_to_direction)

MUNI_IB = pd.read_csv(os.path.join(MUNI_output_dir,'MUNI_IB.csv'))
MUNI_map_IN = MUNI_map[MUNI_map['Direction'] =='IB']
MUNI_map_IN = MUNI_map_IN.rename(columns={"Line": "Route"})
MUNI_IB = MUNI_IB.merge(MUNI_map_IN, on='Route', how='left')

MUNI_map_OUT = MUNI_map[MUNI_map['Direction'] =='OB']
MUNI_map_OUT = MUNI_map_OUT.rename(columns={"Line": "Route"})
MUNI_OB = pd.read_csv(os.path.join(MUNI_output_dir,"MUNI_OB.csv"))
MUNI_OB = MUNI_OB.merge(MUNI_map_OUT, on='Route', how='left')

#GEO info
freeflow = gpd.read_file(FREEFLOW_SHP)
freeflow.crs = 'epsg:2227'
freeflow = freeflow.to_crs(epsg=4236)
node_geo = freeflow[["A", "B", "AB", "geometry"]].copy()
MUNI_IB = MUNI_IB[['Route','Name','Observed', 'Modeled', 'Diff', 'Percentage Diff','AB', 'SEQ', 'Direction']]
muni_ib = MUNI_IB.merge(node_geo, on='AB', how='left').dropna().drop_duplicates()
muni_ib_geo = gpd.GeoDataFrame(muni_ib, geometry='geometry')
# Apply aggregation function using `apply` instead of `agg`
aggregated_muni_ib = muni_ib_geo.groupby('Name').apply(lambda x: pd.Series({
    'Route': x['Route'].iloc[0],
    'Observed': x['Observed'].iloc[0],
    'Modeled': x['Modeled'].iloc[0],
    'Diff': x['Diff'].iloc[0],
    'Percentage Diff': x['Percentage Diff'].iloc[0],
    'AB': x['AB'].iloc[0],
    'Direction': x['Direction'].iloc[0],
    'geometry': concat_ordered_geometries(x)
})).reset_index()

aggregated_muni_ib = aggregated_muni_ib.groupby('Route').apply(lambda x: pd.Series({
    'Observed': x['Observed'].iloc[0],
    'Modeled': x['Modeled'].iloc[0],
    'Diff': x['Diff'].iloc[0],
    'Percentage Diff': x['Percentage Diff'].iloc[0],
    'AB': x['AB'].iloc[0],
    'Direction': x['Direction'].iloc[0],
    'geometry':  x['geometry'].iloc[0],
})).reset_index()

# Convert to GeoDataFrame
aggregated_muni_ib = gpd.GeoDataFrame(aggregated_muni_ib, geometry='geometry')
aggregated_muni_ib.to_file(os.path.join(SHP_file_dir,"muni_ib.shp"))
MUNI_map_IB = aggregated_muni_ib[["Route", "Observed","Modeled","Diff",	"Percentage Diff", 'Direction']].copy()
MUNI_map_IB['Percentage Diff'] = pd.to_numeric(MUNI_map_IB['Percentage Diff'].str.replace('%', '').str.strip(), errors='coerce')
MUNI_map_IB['Percentage Diff'] = MUNI_map_IB['Percentage Diff']/100
# List of columns to convert
columns_to_convert = ['Observed', 'Diff', 'Modeled']
for column in columns_to_convert:
    MUNI_map_IB[column] = pd.to_numeric(MUNI_map_IB[column].str.replace(',', '').str.strip(), errors='coerce')
MUNI_map_IB_ = MUNI_map_IB[['Route', 'Observed', 'Modeled', 'Diff', 'Percentage Diff','Direction']]
MUNI_map_IB = MUNI_map_IB.drop_duplicates()
MUNI_map_IB.to_csv(os.path.join(MUNI_output_dir,"MUNI_map_IB.csv"), index=False)

MUNI_OB = MUNI_OB[['Route', 'Name','Observed', 'Modeled', 'Diff', 'Percentage Diff','AB', 'SEQ', 'Direction']]
muni_ob = MUNI_OB.merge(node_geo, on='AB', how='left').dropna().drop_duplicates()
muni_ob_geo = gpd.GeoDataFrame(muni_ob, geometry='geometry')
# Apply aggregation function using `apply` instead of `agg`
aggregated_muni_ob = muni_ob_geo.groupby('Name').apply(lambda x: pd.Series({
    'Route': x['Route'].iloc[0],
    'Observed': x['Observed'].iloc[0],
    'Modeled': x['Modeled'].iloc[0],
    'Diff': x['Diff'].iloc[0],
    'Percentage Diff': x['Percentage Diff'].iloc[0],
    'AB': x['AB'].iloc[0],
    'Direction': x['Direction'].iloc[0],
    'geometry': concat_ordered_geometries(x)
})).reset_index()

aggregated_muni_ob = aggregated_muni_ob.groupby('Route').apply(lambda x: pd.Series({
    'Observed': x['Observed'].iloc[0],
    'Modeled': x['Modeled'].iloc[0],
    'Diff': x['Diff'].iloc[0],
    'Percentage Diff': x['Percentage Diff'].iloc[0],
    'AB': x['AB'].iloc[0],
    'Direction': x['Direction'].iloc[0],
    'geometry':  x['geometry'].iloc[0],
})).reset_index()

aggregated_muni_ob.to_file(os.path.join(SHP_file_dir,"muni_ob.shp"))
MUNI_map_OB = aggregated_muni_ob[["Route", "Observed","Modeled","Diff",	"Percentage Diff", 'Direction']].copy()
MUNI_map_OB['Percentage Diff'] = pd.to_numeric(MUNI_map_OB['Percentage Diff'].str.replace('%', '').str.strip(), errors='coerce')
MUNI_map_OB['Percentage Diff'] = MUNI_map_OB['Percentage Diff']/100
for column in columns_to_convert:
    MUNI_map_OB[column] = pd.to_numeric(MUNI_map_OB[column].str.replace(',', '').str.strip(), errors='coerce')
MUNI_map_OB = MUNI_map_OB[['Route','Observed', 'Modeled', 'Diff', 'Percentage Diff','Direction']]
MUNI_map_OB = MUNI_map_OB.drop_duplicates()
MUNI_map_OB.to_csv(os.path.join(MUNI_output_dir,"MUNI_map_OB.csv"), index=False)

#BART
BART_boarding_allday = pd.read_csv(os.path.join(BART_output_dir,"BART_boarding_allday.csv"))
obs_BART_line = pd.read_csv(observed_BART)  
model_BART_line = pd.read_csv(os.path.join(OUTPUT_FOLDER,'model_BART.csv'))

def BART_map(type, group_by, TOD, obs_BART_line, model_BART_line, csv, shp):
    obs_condition = pd.Series([True] * len(obs_BART_line))
    model_condition = pd.Series([True] * len(model_BART_line))
    # Processing observed data
    BART_obs = obs_BART_line[obs_condition].groupby(group_by)[type].sum().reset_index()
    BART_obs.rename(columns={type: 'Observed'}, inplace=True)

    # Processing modeled data
    BART_model = model_BART_line[model_condition].groupby(group_by)[type].sum().reset_index()
    BART_model.rename(columns={type: 'Modeled'}, inplace=True)
    
    BART = pd.merge(BART_obs, BART_model, on= group_by, how='left')
    BART['Diff'] = (BART['Modeled'] - BART['Observed'])
    BART['Percentage Diff'] = (BART['Diff'] / BART['Observed'])
    BART['ABS Percentage Diff'] = abs(BART['Percentage Diff'] )
    BART['ABS Diff'] = abs(BART['Diff'] )
    if TOD != None:
        BART = BART[BART['TOD'] == TOD].copy()
    BART.to_csv(os.path.join(BART_output_dir, csv), index=False)
    BART_2 = BART.copy()
    BART_2['Percentage Diff'] = BART_2['Percentage Diff']*100
    numeric_cols = ['Observed', 'Modeled', 'Diff']
    BART_map = format_dataframe(BART_2, numeric_columns=numeric_cols, percentage_columns=['Percentage Diff'])
    BART_map = BART_map.merge(station, on='Station', how='right')
    BART_map = gpd.GeoDataFrame(BART_map, geometry='geometry')
    BART_map.crs = 'epsg:2227'
    BART_map = BART_map.to_crs(epsg=4236)
    BART_map.to_file(os.path.join(SHP_file_dir,shp))
    return BART, BART_map

BART_br, BART_br_map = BART_map('Boardings', ['Station'], None, obs_BART_line, model_BART_line,"BART_br.csv", "BART_br_map.shp")
BART_br_am, BART_br_map_am = BART_map('Boardings', ['Station', 'TOD'], "AM", obs_BART_line, model_BART_line,"BART_br_am.csv", "BART_br_map_am.shp")
BART_br_pm, BART_br_map_pm = BART_map('Boardings', ['Station', 'TOD'], "PM", obs_BART_line, model_BART_line,"BART_br_pm.csv", "BART_br_map_pm.shp")
BART_at, BART_at_map = BART_map('Alightings', ['Station'], None, obs_BART_line, model_BART_line,"BART_at.csv", "BART_at_map.shp")
BART_at_am, BART_at_map_am = BART_map('Alightings', ['Station', 'TOD'], "AM", obs_BART_line, model_BART_line,"BART_at_am.csv", "BART_at_map_am.shp")
BART_at_pm, BART_at_map_pm = BART_map('Alightings', ['Station', 'TOD'], "PM", obs_BART_line, model_BART_line,"BART_at_pm.csv", "BART_at_map_pm.shp")



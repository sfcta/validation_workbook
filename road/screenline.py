import pandas as pd
from pathlib import Path
from transit.utils import format_numeric, format_percentage, dataframe_to_markdown

def process_screenline_data(model_df, observed_df, direction=None, screenline=None, file_path=None):
    """
    Processes and merges modeled and observed screenline data based on given filters.
    
    Parameters:
    - model_df (DataFrame): The DataFrame containing modeled data.
    - observed_df (DataFrame): The DataFrame containing observed data.
    - direction (str, optional): Filter by direction ('Inbound' or 'Outbound').
    - screenline (str, optional): Filter by a specific screenline.

    Returns:
    - DataFrame: The processed and merged DataFrame with total row, difference, and percent difference calculations.
    """
    
    # Apply filters if specified
    if direction:
        model_df = model_df[model_df['Direction'] == direction]
        observed_df = observed_df[observed_df['Direction'] == direction]
    
    if screenline:
        model_df = model_df[model_df['Screenline'] == screenline]
        observed_df = observed_df[observed_df['Screenline'] == screenline]
    
    # Group by 'TOD' and sum values
    model_grouped = model_df.groupby('TOD')['Modeled'].sum().reset_index()
    observed_grouped = observed_df.groupby('TOD')['Observed'].sum().reset_index()
    
    # Merge observed and modeled data
    merged_df = pd.merge(observed_grouped, model_grouped, how='left', on='TOD')
    
    # Calculate total row dynamically
    total_row = pd.DataFrame(merged_df.iloc[:, 1:].sum()).T
    total_row["TOD"] = "Total"
    
    # Append the total row
    merged_df = pd.concat([merged_df, total_row], ignore_index=True)
    
    # Compute difference and percent difference
    tod_order = ["EA", "AM", "MD", "PM", "EV"]
    merged_df['Diff'] = merged_df['Modeled'] - merged_df['Observed']
    merged_df['Percent Diff'] = (merged_df['Diff'] / merged_df['Observed'])*100
    merged_df["TOD"] = pd.Categorical(merged_df["TOD"], categories=tod_order + ["Total"], ordered=True)
    merged_df = merged_df.sort_values("TOD").reset_index(drop=True)
    numeric_cols = ["Observed", "Modeled", "Diff"]
    for col in numeric_cols:
        merged_df[col] = merged_df[col].apply(lambda x: format_numeric(x))
        
    merged_df['Percent Diff'] = merged_df['Percent Diff'].apply(lambda x: format_percentage(x))
    merged_df[~merged_df["TOD"].isin(["Total"])].to_csv(file_path)
    
    return merged_df

def generate_screenline_data(obs_filepath, counts_modeled, dir_path):
    df = pd.read_csv(obs_filepath)
    df_modeled = pd.read_csv(counts_modeled)
    df_observed = df[df['Screenline'].notna()]
    df_screenline_node = df_observed[['A', 'B', 'Screenline','Direction']]
    df_merged = df_screenline_node.merge(df_modeled, on=['A', 'B'], how='left')
    model_screenline = df_merged[['Screenline', 'Direction', 'EA_est', 'AM_est', 'MD_est', 'PM_est', 'EV_est']].rename(
        columns={
            'AM_est': 'AM',
            'MD_est': 'MD',
            'PM_est': 'PM',
            'EV_est': 'EV',
            'EA_est': 'EA'
        }
    )

    observed_screenline = df_merged[['Screenline', 'Direction', 'EA_obs', 'AM_obs', 'MD_obs', 'PM_obs', 'EV_obs']].rename(
        columns={
            'AM_obs': 'AM',
            'MD_obs': 'MD',
            'PM_obs': 'PM',
            'EV_obs': 'EV',
            'EA_obs': 'EA'
        }
    )
    model_screenline = model_screenline.melt(id_vars=["Screenline", "Direction"], 
                        var_name="TOD", 
                        value_name="Modeled")
    observed_screenline = observed_screenline.melt(id_vars=["Screenline", "Direction"], 
                        var_name="TOD", 
                        value_name="Observed")
    screenline_ib = process_screenline_data(model_screenline, observed_screenline, direction="Inbound", file_path=dir_path + "/screenline_ib.csv")
    screenline_ob = process_screenline_data(model_screenline, observed_screenline, direction="Outbound", file_path=dir_path + "/screenline_ob.csv")
    baybridge_screenline_ib = process_screenline_data(model_screenline, observed_screenline, direction="Inbound", screenline="Bay Bridge", file_path=dir_path + "/baybridge_screenline_ib.csv")
    baybridge_screenline_ob = process_screenline_data(model_screenline, observed_screenline, direction="Outbound", screenline="Bay Bridge", file_path=dir_path + "/baybridge_screenline_ob.csv")
    ggb_screenline_ib = process_screenline_data(model_screenline, observed_screenline, direction="Inbound", screenline="Golden Gate Bridge", file_path=dir_path + "/ggb_screenline_ib.csv")
    ggb_screenline_ob = process_screenline_data(model_screenline, observed_screenline, direction="Outbound", screenline="Golden Gate Bridge", file_path=dir_path + "/ggb_screenline_ob.csv")
    sfsm_screenline_ib = process_screenline_data(model_screenline, observed_screenline, direction="Inbound", screenline="San Mateo County Line", file_path=dir_path + "/sfsm_screenline_ib.csv")
    sfsm_screenline_ob = process_screenline_data(model_screenline, observed_screenline, direction="Outbound", screenline="San Mateo County Line", file_path=dir_path + "/sfsm_screenline_ob.csv")
    dataframe_to_markdown(
    screenline_ib,
    file_name=dir_path + "/screenline_ib.md",
    highlight_rows=[len(screenline_ib) - 1],
    center_align_columns=None,
    column_widths=70,
    )
    dataframe_to_markdown(
        screenline_ob,
        file_name=dir_path + "/screenline_ob.md",
        highlight_rows=[len(screenline_ib) - 1],
        center_align_columns=None,
        column_widths=70,
    )
    dataframe_to_markdown(
        baybridge_screenline_ib,
        file_name=dir_path + "/baybridge_screenline_ib.md",
        highlight_rows=[len(screenline_ib) - 1],
        center_align_columns=None,
        column_widths=70,
    )
    dataframe_to_markdown(
        baybridge_screenline_ob,
        file_name=dir_path + "/baybridge_screenline_ob.md",
        highlight_rows=[len(screenline_ib) - 1],
        center_align_columns=None,
        column_widths=70,
    )
    dataframe_to_markdown(
        ggb_screenline_ib,
        file_name=dir_path + "/ggb_screenline_ib.md",
        highlight_rows=[len(screenline_ib) - 1],
        center_align_columns=None,
        column_widths=70,
    )
    dataframe_to_markdown(
        ggb_screenline_ob,
        file_name=dir_path + "/ggb_screenline_ob.md",
        highlight_rows=[len(screenline_ib) - 1],
        center_align_columns=None,
        column_widths=70,
    )
    dataframe_to_markdown(
        sfsm_screenline_ib,
        file_name=dir_path + "/sfsm_screenline_ib.md",
        highlight_rows=[len(screenline_ib) - 1],
        center_align_columns=None,
        column_widths=70,
    )
    dataframe_to_markdown(
        sfsm_screenline_ob,
        file_name=dir_path + "/sfsm_screenline_ob.md",
        highlight_rows=[len(screenline_ib) - 1],
        center_align_columns=None,
        column_widths=70,
    )


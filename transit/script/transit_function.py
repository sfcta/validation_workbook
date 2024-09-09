
def read_dbf_and_groupby_sum(dbf_file, system_filter, groupby_columns, sum_column):
    """
    Reads a DBF file, filters by SYSTEM, group by specified columns,
    and calculates sum of a specified column.

    Parameters:
    dbf_file_path (str): The path to the DBF file.
    system_filter (str): The value to filter by on the 'SYSTEM' column.
    groupby_columns (list): The list of columns to group by.
    sum_column (str): The column on which to calculate the sum.

    Returns:
    DataFrame: Pandas DataFrame with the groupby and sum applied.
    """
    filtered_df = dbf_file[dbf_file["SYSTEM"] == system_filter]  # filter on SYSTEM columns
    # group by `groupby_columns` and sum `sum_column`
    grouped_sum = filtered_df.groupby(groupby_columns)[sum_column].sum()
    # reset index to convert it back to a DataFrame
    grouped_sum_df = grouped_sum.reset_index()
    return grouped_sum_df

def dataframe_to_markdown(
    df,
    file_name="dataframe_table.md",
    highlight_rows=None,
    center_align_columns=None,
    column_widths=100,
):
    """
    Convert a Pandas DataFrame to a custom Markdown table, highlight specific rows,
    right align specified columns, and save it to a file, with the first column always
    left-aligned in both header and data.

    Parameters:
    df (pd.DataFrame): The DataFrame to convert.
    file_name (str): Name of the file to save the Markdown table.
    highlight_rows (list): List of row indices to highlight.
    right_align_columns (list): List of column names to right align.
    """
    if highlight_rows is None:
        highlight_rows = []
    if center_align_columns is None:
        center_align_columns = []

    # Start the Markdown table with the header
    md_output = "<table>\n<thead>\n<tr>\n"
    for i, col in enumerate(df.columns):
        # Left align for the first column header, center align for others
        header_align = "left" if i == 0 else "center"
        md_output += f'<th style="text-align:{header_align}; width: {column_widths}px;"><strong>{col}</strong></th>\n'
    md_output += "</tr>\n</thead>\n<tbody>\n"

    # Add the table rows
    for index, row in df.iterrows():
        md_output += "<tr>\n"
        for i, col in enumerate(df.columns):
            cell_value = "" if pd.isna(row[col]) else row[col]

            # Determine the alignment based on the column name and index
            if i == 0:
                align = "left"  # Left align for the first column
            elif col in center_align_columns:
                align = "center"  # Right align for specified columns
            else:
                align = "right"  # Center align for other columns

            # Apply highlight if the row index is in the highlight_rows list
            if index in highlight_rows:
                md_output += f'<td style="text-align:{align}"><strong>{cell_value}</strong></td>\n'
            else:
                md_output += f'<td style="text-align:{align}">{cell_value}</td>\n'
        md_output += "</tr>\n"

    md_output += "</tbody>\n</table>"

    # Save to a Markdown file
    with open(file_name, "w") as file:
        file.write(md_output)

    print(f"Markdown table saved to '{file_name}'")
    
def dataframe_to_markdown(
    df,
    file_name="dataframe_table.md",
    highlight_rows=None,
    center_align_columns=None,
    column_widths=100,
):
    """
    Convert a Pandas DataFrame to a custom Markdown table, highlight specific rows,
    right align specified columns, and save it to a file, with the first column always
    left-aligned in both header and data.

    Parameters:
    df (pd.DataFrame): The DataFrame to convert.
    file_name (str): Name of the file to save the Markdown table.
    highlight_rows (list): List of row indices to highlight.
    right_align_columns (list): List of column names to right align.
    """
    if highlight_rows is None:
        highlight_rows = []
    if center_align_columns is None:
        center_align_columns = []

    # Start the Markdown table with the header
    md_output = "<table>\n<thead>\n<tr>\n"
    for i, col in enumerate(df.columns):
        # Left align for the first column header, center align for others
        header_align = "left" if i == 0 else "center"
        md_output += f'<th style="text-align:{header_align}; width: {column_widths}px;"><strong>{col}</strong></th>\n'
    md_output += "</tr>\n</thead>\n<tbody>\n"

    # Add the table rows
    for index, row in df.iterrows():
        md_output += "<tr>\n"
        for i, col in enumerate(df.columns):
            cell_value = "" if pd.isna(row[col]) else row[col]

            # Determine the alignment based on the column name and index
            if i == 0:
                align = "left"  # Left align for the first column
            elif col in center_align_columns:
                align = "center"  # Right align for specified columns
            else:
                align = "right"  # Center align for other columns

            # Apply highlight if the row index is in the highlight_rows list
            if index in highlight_rows:
                md_output += f'<td style="text-align:{align}"><strong>{cell_value}</strong></td>\n'
            else:
                md_output += f'<td style="text-align:{align}">{cell_value}</td>\n'
        md_output += "</tr>\n"

    md_output += "</tbody>\n</table>"

    # Save to a Markdown file
    with open(file_name, "w") as file:
        file.write(md_output)

    print(f"Markdown table saved to '{file_name}'")
    
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
    formatted_df = df.fillna("-")

    # Format specified numeric columns
    for col in numeric_columns:
        formatted_df[col] = formatted_df[col].apply(lambda x: format_numeric(x))

    # Format percentage columns
    for col in percentage_columns:
        formatted_df[col] = formatted_df[col].apply(lambda x: format_percentage(x))

    return formatted_df


def format_numeric(x):
    """Format a numeric value with commas and no decimal places."""
    try:
        return f"{float(x):,.0f}" if x not in ["-", ""] else x
    except ValueError:
        return x


def format_percentage(x):
    """Format a value as a percentage."""
    try:
        return f"{float(x):.0f}%" if x not in ["-", ""] else x
    except ValueError:
        return x
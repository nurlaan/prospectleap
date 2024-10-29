import ibis
import pandas as pd
from typing import Dict, List, Union, Any

# Function to update value in SQL table
def update_dbvalue(db_path: str, 
                   table_name: str, 
                   search_column: str, 
                   update_column: str, 
                   search_value: str, 
                   value: Union[str, int]
                   ) -> None:
    """
    Update value in SQL table based on search value.
    
    Args:
        db_path: path to the database
        table: table to be updated
        search_column: column that includes search_values
        search_value: criteria to be searched
        update_column: column where values to be updated
        value: value to replace old value
    
    Returns:
        None
    """
    try:
        # Connect to database
        conn = ibis.sqlite.connect(db_path)

        # Check if table exists
        if table_name not in conn.list_tables():
            raise ValueError(f"Table '{table_name}' not found in database")
       
        # Get table schema to check columns
        table = conn.table(table_name)
        available_columns = table.columns

        # Check if required column exist
        missing_columns = []
        if search_column not in available_columns:
            missing_columns.append(search_column)
        if update_column not in available_columns:
            missing_columns.append(update_column)

        if missing_columns:
            raise ValueError(f"Columns {missing_columns} not found in table '{table_name}'. Available columns are: {available_columns}")

        # SQL statement to update value
        conn.raw_sql(f"""
            UPDATE {table_name}
            SET {update_column} = '{value}'
            WHERE {search_column} = '{search_value}'
        """)

        conn.con.commit()

    except Exception as e:
        print(f"Error occured: {str(e)}")
        raise

# function to check values in table
def check_dbvalue(
    db_path: str,
    table_name: str,
    filter_column: str,
    filter_value: str,
    check_column: str,
    check_value: Any
    ):
    """
    Checks value in SQL table against provided value based on filter column and filter value

    Args:
        db_path: Path to the database
        table_name: SQL table
        filter_column: Column that to be filtered 
        filter_value: Value to be searched in filter column
        check_column: Column where value to be checked is located
        check_value: value that needs to be compared against value in check column
    
    Returns:
        Boolean value: True if the check value equals to the value in check column and False if not
    """
    try:
        # Connect to database
        conn = ibis.sqlite.connect(db_path)

        # Table expression
        table = conn.table(table_name)

        # Filtering SQL table and transforming to pandas
        table_filtered = table.filter(table[filter_column] == filter_value).execute()

        # Accessing filter_value
        filter_value_element = table_filtered[check_column].iloc[0]

        if filter_value_element == check_value:
            return True
        else:
            return False

    except Exception as e:
        print(f"Error: {str(e)}")
        return 1


# Function to extract table from SQL and parse it to pandas
def get_table(db_path: str, 
              table_name: str
              ) -> pd.DataFrame:
    """
    Extract table from SQLite database and return as pandas DataFrame

    Args:
        db_path: Path to the database
        table_name: Name of the table to extract

    Returns:
        pd.DataFrame: Table data as pandas DataFrame
    """
    try:
        # Connect to database
        conn = ibis.sqlite.connect(db_path)
        
        # Check if table exists
        if table_name not in conn.list_tables():
            raise ValueError(f"Table '{table_name}' not found in database")

        # Get table
        table = conn.table(table_name)
        
        # Convert to DataFrame and return
        return table.execute()

    except Exception as e:
        print(f"Error occured: {str(e)}")
        raise

# Function to filter pandas DataFrame
def filter_table(
        df: pd.DataFrame,
        column_name: str,
        filter_values: Union[List, str, int, float],
    ) -> pd.DataFrame:
    """
    Filter table from SQL database on specified column and value(s).

    Args:
        df: Input pd.Dataframe to be filtered
        column_name: Name of the column to filter on
        filter_values: Single value or list of values to filter by
    
    Returns:
        filtered table as a pandas DataFrame
    """

    # Input validation
    if column_name not in df.columns:
        raise ValueError(f"Column '{column_name}' not found in DataFrame.")

    # Convert single value to list for consistent processing
    if not isinstance(filter_values, list):
        filter_values = [filter_values]

    # Apply filter
    filtered_df = df[df[column_name].isin(filter_values)]
    
    return filtered_df.copy()

# Function to count total number of rows in table
def count_total_rows(df: pd.DataFrame) -> int:
    """
    Count total rows in a table.

    Args:
        df: Input table (as a pandas DataFrame) which rows need to be counted

    Returns:
        Total number of rows in a table
    """

    # Input validation
    if not isinstance(df, pd.DataFrame):
        raise ValueError(f"Input must be a pandas DataFrame")

    total_rows = len(df)

    return total_rows

# Function that provides short information about the table
def table_info(df: pd.DataFrame, 
               column_name: str,
               include_percentages: bool = True
               ) -> str:
    """
    Provides distribution info about the table based on specific column.
    
    Args:
        df: Input DataFrame to analyze
        column_name: Name of the column to analyze
        include_percentages: Whether to include precentages in the output (default=True)
    
    Returns:
        Formatted string containing the analysis results
    """

    # Input validation
    if not isinstance(df, pd.DataFrame):
        raise ValueError(f"Input must be a pandas DataFrame")

    if column_name not in df.columns:
        raise ValueError(f"Column '{column_name}' not found in DataFrame")

    # Get total number of rows
    total_rows = count_total_rows(df)

    # Get value counts including NA values
    value_counts = df[column_name].value_counts(dropna=False)

    # Build the output string
    output = []
    output.append("*"*50)
    output.append(f"Total number of rows: {total_rows}\n")
    output.append(f"Distribution for column '{column_name}':")

    # Add each value's count and percentage
    for value, count in value_counts.items():
        value_str = 'NA' if pd.isna(value) else str(value)
        if include_percentages:
            percentage = (count / total_rows) * 100
            output.append(f"- {value_str}: {count} rows ({percentage:.1f}%)")
        else:
            output.append(f"- {value_str}: {count} rows")
    
    output.append("*"*50)
    
    return "\n".join(output)


# function to populate SQL table
def finviz_table_populate(db_path: str, data: List[Dict[str, str]]) -> None:
    """
    Populates 'finviz' table with data received in the JSON format or list of dictionaries 

    Args:
        db_path: Path to the database
        data: Data in the JSON format: [{'ticker':'str','date':'str', 'title':'str', 'link':'str'}...]

    Returns:
        n/a
    """
    # Connect to database
    conn = ibis.sqlite.connect(db_path)

    # Insert data
    conn.insert(table_name='finviz', obj=data)


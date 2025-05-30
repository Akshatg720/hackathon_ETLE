import pandas as pd
import mysql.connector
from typing import Dict, List, Union
import json
from datetime import datetime
import numpy as np

def get_nested_value(data: Dict, key_path: str) -> any:
    """
    Get value from nested dictionary using dot notation.
    
    Args:
        data: Dictionary to extract value from
        key_path: Path to the value using dot notation (e.g., 'person.age')
    
    Returns:
        The value at the specified path or None if not found
    """
    if not data:
        return None
        
    keys = key_path.split('.')
    current = data
    
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return None
            
    return current

def build_where_clause(conditions: List[Dict]) -> str:
    """
    Build WHERE clause from conditions.
    
    Args:
        conditions: List of condition dictionaries
    
    Returns:
        str: WHERE clause string
    """
    if not conditions:
        return ""
        
    where_parts = []
    for condition in conditions:
        column = condition['column']
        operator = condition['operator']
        value = condition['value']
        
        # Handle different value types
        if isinstance(value, str):
            value = f"'{value}'"
        elif value is None:
            value = "NULL"
            
        where_parts.append(f"{column} {operator} {value}")
    
    return " WHERE " + " AND ".join(where_parts)

def build_order_by_clause(order_by: List[Dict]) -> str:
    """
    Build ORDER BY clause.
    
    Args:
        order_by: List of order by dictionaries
    
    Returns:
        str: ORDER BY clause string
    """
    if not order_by:
        return ""
        
    order_parts = []
    for order in order_by:
        column = order['column']
        direction = order.get('direction', 'ASC')
        order_parts.append(f"{column} {direction}")
    
    return " ORDER BY " + ", ".join(order_parts)

def build_limit_clause(limit: Dict) -> str:
    """
    Build LIMIT clause.
    
    Args:
        limit: Limit configuration dictionary
    
    Returns:
        str: LIMIT clause string
    """
    if not limit:
        return ""
        
    offset = limit.get('offset', 0)
    count = limit.get('count')
    
    if count is None:
        return ""
        
    if offset > 0:
        return f" LIMIT {offset}, {count}"
    return f" LIMIT {count}"

def extract_from_mysql(config: Dict) -> pd.DataFrame:
    """
    Extract data from MySQL database based on configuration.
    
    Args:
        config: Dictionary containing MySQL connection and column configuration
    
    Returns:
        pd.DataFrame: Extracted data as a pandas DataFrame
    """
    # Extract MySQL connection details
    mysql_config = config['source']
    connection = mysql.connector.connect(
        host=mysql_config['host'],
        port=mysql_config['port'],
        database=mysql_config['database'],
        user=mysql_config['user'],
        password=mysql_config['password']
    )
    
    try:
        # First, let's check if the table exists and has data
        cursor = connection.cursor(dictionary=True)
        test_query = f"SELECT COUNT(*) as count FROM {mysql_config['table']}"
        print("Executing test query:", test_query)
        cursor.execute(test_query)
        result = cursor.fetchone()
        print(f"Number of rows in table: {result['count']}")
        
        # Get column names and types
        columns = config['columns']
        column_names = [col['name'] for col in columns]
        
        # Build the base SELECT query
        query = f"SELECT {', '.join(column_names)} FROM {mysql_config['table']}"
        
        # Add WHERE clause if conditions are specified
        if 'where' in config:
            where_parts = []
            for where_item in config['where']:
                condition = where_item['condition']
                column = condition['column']
                operator = condition['operator']
                value = condition['value']
                
                # Check if this is a JSON field condition
                json_field = None
                for col_config in columns:
                    if col_config['type'] == 'json' and 'keys' in col_config:
                        for key_config in col_config['keys']:
                            if isinstance(key_config, dict) and key_config.get('output_name') == column:
                                json_field = {
                                    'column': col_config['name'],
                                    'path': key_config['path']
                                }
                                break
                
                # Handle different value types
                if isinstance(value, str):
                    # For JSON numeric fields, don't quote the value
                    if json_field and any(key_config.get('output_name') == column and 
                                        key_config.get('type', 'string') == 'numeric' 
                                        for col_config in columns 
                                        for key_config in col_config.get('keys', [])):
                        value = value  # Keep as unquoted number
                    else:
                        value = f"'{value}'"
                elif value is None:
                    value = "NULL"
                
                # If it's a JSON field, use JSON_EXTRACT with double quotes for the path
                if json_field:
                    # For numeric JSON fields, use CAST to ensure proper comparison
                    if any(key_config.get('output_name') == column and 
                          key_config.get('type', 'string') == 'numeric' 
                          for col_config in columns 
                          for key_config in col_config.get('keys', [])):
                        where_parts.append(f"CAST(JSON_EXTRACT({json_field['column']}, \"$.{json_field['path']}\") AS DECIMAL(10,2)) {operator} {value}")
                    else:
                        where_parts.append(f"JSON_EXTRACT({json_field['column']}, \"$.{json_field['path']}\") {operator} {value}")
                else:
                    where_parts.append(f"{column} {operator} {value}")
            
            if where_parts:
                query += " WHERE " + " AND ".join(where_parts)
        
        # Add ORDER BY clause if specified
        if 'order_by' in config:
            query += build_order_by_clause(config['order_by'])
        
        # Add LIMIT clause if specified
        if 'limit' in config:
            query += build_limit_clause(config['limit'])
        
        # Print the query for debugging
        print("Executing SQL query:", query)
        
        # Execute query and fetch data
        cursor.execute(query)
        data = cursor.fetchall()
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Print column names and first few rows for debugging
        print("\nAvailable columns in DataFrame:", df.columns.tolist())
        if not df.empty:
            print("\nFirst few rows of data:")
            print(df.head())
        else:
            print("\nNo data returned from query!")
        
        # Process each column based on its type
        for col_config in columns:
            col_name = col_config['name']
            col_type = col_config['type']
            output_name = col_config.get('output_name', col_name)
            
            # Find the actual column name in the DataFrame (case-insensitive)
            actual_col_name = next((col for col in df.columns if col.lower() == col_name.lower()), None)
            if actual_col_name is None:
                print(f"Warning: Column '{col_name}' not found in DataFrame. Available columns: {df.columns.tolist()}")
                continue
            
            if col_type == 'date':
                df[output_name] = pd.to_datetime(df[actual_col_name])
            
            elif col_type == 'numeric':
                if col_config['data_type'] == 'int':
                    df[output_name] = pd.to_numeric(df[actual_col_name], errors='coerce').astype('Int64')
                else:  # float
                    df[output_name] = pd.to_numeric(df[actual_col_name], errors='coerce')
            
            elif col_type == 'json':
                if 'keys' in col_config:
                    # Extract specific keys from JSON
                    for key_config in col_config['keys']:
                        # Handle both string and dictionary key configurations
                        if isinstance(key_config, str):
                            key_path = key_config
                            new_col_name = f"{output_name}_{key_path.replace('.', '_')}"
                        else:
                            key_path = key_config['path']
                            new_col_name = key_config.get('output_name', f"{output_name}_{key_path.replace('.', '_')}")
                        
                        df[new_col_name] = df[actual_col_name].apply(
                            lambda x: get_nested_value(json.loads(x), key_path) if x else None
                        )
                    # Drop the original JSON column
                    df = df.drop(columns=[actual_col_name])
            
            elif col_type == 'string':
                df[output_name] = df[actual_col_name]
            
            # If output name is different from input name, drop the original column
            if output_name != actual_col_name and col_type != 'json':
                df = df.drop(columns=[actual_col_name])
        
        # Print final DataFrame info
        print("\nFinal DataFrame info:")
        print("Columns:", df.columns.tolist())
        print("Shape:", df.shape)
        if not df.empty:
            print("\nFirst few rows of processed data:")
            print(df.head())
        
        return df
        
    finally:
        connection.close()

def extract(config: Union[str, Dict]) -> pd.DataFrame:
    """
    Extract data from the specified source based on configuration.
    
    Args:
        config: Either a JSON string or dictionary containing extraction configuration
    
    Returns:
        pd.DataFrame: Extracted data as a pandas DataFrame
    """
    # Convert string to dict if JSON string is provided
    if isinstance(config, str):
        config = json.loads(config)
    
    source_type = config['source']['type']
    
    if source_type == 'mysql':
        return extract_from_mysql(config)
    else:
        raise ValueError(f"Unsupported source type: {source_type}")

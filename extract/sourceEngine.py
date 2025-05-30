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
        # Get column names and types
        columns = config['columns']
        column_names = [col['name'] for col in columns]
        
        # Build the base SELECT query
        query = f"SELECT {', '.join(column_names)} FROM {mysql_config['table']}"
        
        # Add WHERE clause if conditions are specified
        if 'conditions' in config:
            query += build_where_clause(config['conditions'])
        
        # Add ORDER BY clause if specified
        if 'order_by' in config:
            query += build_order_by_clause(config['order_by'])
        
        # Add LIMIT clause if specified
        if 'limit' in config:
            query += build_limit_clause(config['limit'])
        
        # Execute query and fetch data
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query)
        data = cursor.fetchall()
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Process each column based on its type
        for col_config in columns:
            col_name = col_config['name']
            col_type = col_config['type']
            output_name = col_config.get('output_name', col_name)
            
            if col_type == 'date':
                df[output_name] = pd.to_datetime(df[col_name])
            
            elif col_type == 'numeric':
                if col_config['data_type'] == 'int':
                    df[output_name] = pd.to_numeric(df[col_name], errors='coerce').astype('Int64')
                else:  # float
                    df[output_name] = pd.to_numeric(df[col_name], errors='coerce')
            
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
                        
                        df[new_col_name] = df[col_name].apply(
                            lambda x: get_nested_value(json.loads(x), key_path) if x else None
                        )
                    # Drop the original JSON column
                    df = df.drop(columns=[col_name])
            
            elif col_type == 'string':
                df[output_name] = df[col_name]
            
            # If output name is different from input name, drop the original column
            if output_name != col_name and col_type != 'json':
                df = df.drop(columns=[col_name])
        
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

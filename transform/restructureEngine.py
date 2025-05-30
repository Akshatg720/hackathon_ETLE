import pandas as pd
from typing import Dict, Union, List
import json
from pathlib import Path
from datetime import datetime

def datetime_handler(obj):
    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

def restructure(export_config: Union[str, Dict]) -> Union[pd.DataFrame, Dict, str]:
    """
    Restructure and export the global dataframe according to the provided configuration.
    
    Args:
        export_config: Either a JSON string or dictionary containing export configuration
    
    Returns:
        The restructured data in the specified format
    """
    from .transformEngine import df  # Import the global dataframe
    
    if df is None:
        raise ValueError("No dataframe available. Please set the dataframe first.")
    
    # Convert string to dict if JSON string is provided
    if isinstance(export_config, str):
        export_config = json.loads(export_config)
    
    export_format = export_config.get('format', 'dataframe')
    output_path = export_config.get('output_path')
    
    # Initialize df_export with the full dataframe
    df_export = df.copy()
    
    # Handle column selection and renaming
    columns_config = export_config.get('columns', {})
    if columns_config:
        # If columns is a list, just select those columns
        if isinstance(columns_config, list):
            df_export = df[columns_config]
        # If columns is a dict, rename columns while selecting
        elif isinstance(columns_config, dict):
            df_export = df[list(columns_config.keys())].rename(columns=columns_config)
    
    # Handle JSON structure configuration
    json_structure = export_config.get('json_structure')
    
    if export_format == 'json':
        if json_structure:
            # Convert to structured JSON format
            result = convert_to_structured_json(df_export, json_structure)
        else:
            # Convert to simple JSON (list of records)
            result = df_export.to_dict(orient='records')
            
        # Write to file if output_path is provided
        if output_path:
            with open(output_path, 'w') as f:
                json.dump(result, f, indent=2, default=datetime_handler)
        return result
        
    elif export_format == 'csv':
        if output_path:
            df_export.to_csv(output_path, index=False)
        return df_export
        
    elif export_format == 'excel':
        if output_path:
            df_export.to_excel(output_path, index=False)
        return df_export
        
    else:  # dataframe
        return df_export

def convert_to_structured_json(df: pd.DataFrame, structure: Dict) -> List[Dict]:
    """
    Convert dataframe to structured JSON format based on the provided configuration.
    
    Args:
        df: pandas DataFrame to convert
        structure: Dictionary defining the desired JSON structure
    
    Returns:
        List of dictionaries containing the structured JSON
    """
    result = []
    
    for _, row in df.iterrows():
        item = {}
        
        # Handle root level fields
        if 'root' in structure:
            for field in structure['root']:
                if field in df.columns:
                    item[field] = row[field]
        
        # Handle nested fields
        if 'nested' in structure:
            for nested_key, nested_fields in structure['nested'].items():
                nested_data = {}
                for field in nested_fields:
                    if field in df.columns:
                        nested_data[field] = row[field]
                if nested_data:  # Only add if there are fields
                    item[nested_key] = nested_data
        
        result.append(item)
    
    return result

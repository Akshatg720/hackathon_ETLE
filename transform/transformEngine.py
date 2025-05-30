import pandas as pd
from typing import Dict, List, Union
import json
import importlib.util
import sys
from pathlib import Path

# Global variable to store the dataframe
df = None

def set_dataframe(dataframe: pd.DataFrame) -> None:
    """Set the global dataframe that will be transformed."""
    global df
    df = dataframe

def load_custom_module(file_path: str):
    """
    Dynamically load a Python module from a file path.
    
    Args:
        file_path: Path to the Python file
        
    Returns:
        module: The loaded Python module
    """
    # Convert to absolute path if relative
    if not Path(file_path).is_absolute():
        file_path = str(Path.cwd() / file_path)
    
    # Load the module
    spec = importlib.util.spec_from_file_location("custom_module", file_path)
    if spec is None:
        raise ValueError(f"Could not load module from {file_path}")
    
    module = importlib.util.module_from_spec(spec)
    sys.modules["custom_module"] = module
    spec.loader.exec_module(module)
    return module

def transform(transform_config: Union[str, Dict]) -> pd.DataFrame:
    """
    Apply transformations to the global dataframe based on the provided configuration.
    
    Args:
        transform_config: Either a JSON string or dictionary containing transformation rules
    
    Returns:
        pd.DataFrame: Transformed dataframe
    """
    global df
    
    if df is None:
        raise ValueError("No dataframe set. Use set_dataframe() first.")
    
    # Convert string to dict if JSON string is provided
    if isinstance(transform_config, str):
        transform_config = json.loads(transform_config)
    
    transformations = transform_config.get('transformations', [])
    
    for transform in transformations:
        transform_type = transform['type']
        columns = transform['columns']
        output_column = transform['output_column']
        
        if transform_type == 'arithmetic':
            operation = transform['operation']
            if operation == 'add':
                df[output_column] = df[columns[0]] + df[columns[1]]
            elif operation == 'multiply':
                df[output_column] = df[columns[0]] * df[columns[1]]
            elif operation == 'subtract':
                df[output_column] = df[columns[0]] - df[columns[1]]
            elif operation == 'divide':
                df[output_column] = df[columns[0]] / df[columns[1]]
                
        elif transform_type == 'aggregate':
            operation = transform['operation']
            group_by = transform.get('group_by', [])
            if operation == 'sum':
                agg_df = df.groupby(group_by)[columns].sum()
                agg_df.columns = [output_column]
                df = df.merge(agg_df, on=group_by, how='left')
            elif operation == 'mean':
                agg_df = df.groupby(group_by)[columns].mean()
                agg_df.columns = [output_column]
                df = df.merge(agg_df, on=group_by, how='left')
            elif operation == 'count':
                agg_df = df.groupby(group_by)[columns].count()
                agg_df.columns = [output_column]
                df = df.merge(agg_df, on=group_by, how='left')
                
        elif transform_type == 'custom_file':
            file_path = transform['file_path']
            function_name = transform['function_name']
            parameters = transform.get('parameters', {})
            
            try:
                # Load the custom module
                custom_module = load_custom_module(file_path)
                
                # Get the function from the module
                if not hasattr(custom_module, function_name):
                    raise ValueError(f"Function {function_name} not found in {file_path}")
                
                custom_func = getattr(custom_module, function_name)
                
                # Apply the function to the columns
                if len(columns) == 1:
                    df[output_column] = custom_func(df[columns[0]], **parameters)
                else:
                    df[output_column] = custom_func(*[df[col] for col in columns], **parameters)
                    
            except Exception as e:
                raise ValueError(f"Error in custom transformation {function_name} from {file_path}: {str(e)}")
    
    return df
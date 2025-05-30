import json
import sys
from pathlib import Path
from extract.sourceEngine import extract
from transform.transformEngine import transform, set_dataframe
from transform.restructureEngine import restructure
from load.loadEngine import load
import pandas as pd

def run_etl(config_file: str) -> None:
    """
    Run the ETL process based on the configuration file.
    
    Args:
        config_file: Path to the JSON configuration file
    """
    # Read and parse the configuration file
    with open(config_file, 'r') as f:
        config = json.load(f)
    
    try:
        # Extract phase
        print("Starting Extract phase...")
        df = extract(config['extract'])
        print("Extract phase completed successfully.")
        
        # Set the dataframe for transform
        set_dataframe(df)
        
        # Transform phase
        if 'transform' in config:
            print("Starting Transform phase...")
            df = transform(config['transform'])
            print("Transform phase completed successfully.")
        
        # Restructure phase
        if 'restructure' in config:
            print("Starting Restructure phase...")
            df = restructure(config['restructure'])
            print("Restructure phase completed successfully.")
        
        # Load phase
        print("Starting Load phase...")
        if isinstance(df, pd.DataFrame):
            # If the output is a DataFrame, save it to a temporary file
            temp_file = "temp_output.csv"
            df.to_csv(temp_file, index=False)
            load(temp_file, config['load'])
            # Clean up temporary file
            Path(temp_file).unlink()
        else:
            # If the output is already a file (from restructure)
            load(df, config['load'])
        print("Load phase completed successfully.")
        
        print("ETL process completed successfully!")
        
    except Exception as e:
        print(f"Error during ETL process: {str(e)}")
        raise

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python main.py <config_file>")
        sys.exit(1)
    
    config_file = sys.argv[1]
    run_etl(config_file)
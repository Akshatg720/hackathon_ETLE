import boto3
from typing import Dict, Union
import json
import os
from pathlib import Path

def load_to_s3(file_path: str, config: Dict) -> None:
    """
    Load file to S3 bucket.
    
    Args:
        file_path: Path to the file to be uploaded
        config: Dictionary containing S3 configuration
    """
    s3_config = config['destination']
    
    # Initialize S3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=s3_config['aws_access_key_id'],
        aws_secret_access_key=s3_config['aws_secret_access_key'],
        region_name=s3_config.get('region', 'us-east-1')
    )
    
    # Upload to S3
    bucket_name = s3_config['bucket']
    s3_key = s3_config['key']
    
    s3_client.upload_file(
        file_path,
        bucket_name,
        s3_key
    )

def load_to_local(file_path: str, config: Dict) -> None:
    """
    Load file to local file system.
    
    Args:
        file_path: Path to the file to be moved
        config: Dictionary containing local file system configuration
    """
    local_config = config['destination']
    destination_path = local_config['path']
    
    # Create directory if it doesn't exist
    directory = os.path.dirname(destination_path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    
    # Move the file
    os.rename(file_path, destination_path)

def load(file_path: str, config: Union[str, Dict]) -> None:
    """
    Load file to the specified destination based on configuration.
    
    Args:
        file_path: Path to the file to be moved
        config: Either a JSON string or dictionary containing load configuration
    
    Example config for S3:
    {
        "destination": {
            "type": "s3",
            "aws_access_key_id": "your_access_key",
            "aws_secret_access_key": "your_secret_key",
            "region": "us-east-1",
            "bucket": "your-bucket",
            "key": "path/to/file.csv"
        }
    }
    
    Example config for local:
    {
        "destination": {
            "type": "local",
            "path": "/path/to/output/file.csv"
        }
    }
    """
    # Convert string to dict if JSON string is provided
    if isinstance(config, str):
        config = json.loads(config)
    
    # Validate file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Source file not found: {file_path}")
    
    destination_type = config['destination']['type']
    
    if destination_type == 's3':
        load_to_s3(file_path, config)
    elif destination_type == 'local':
        load_to_local(file_path, config)
    else:
        raise ValueError(f"Unsupported destination type: {destination_type}")

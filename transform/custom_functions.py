import pandas as pd
from datetime import datetime

def calculate_duration_hours(start_time: pd.Series, end_time: pd.Series) -> pd.Series:
    """
    Calculate the duration in hours between two datetime columns.
    
    Args:
        start_time: Series containing start times
        end_time: Series containing end times
        
    Returns:
        Series containing duration in hours
    """
    return (end_time - start_time).dt.total_seconds() / 3600

def convert_to_usd(amount: pd.Series, conversion_rate: float = 0.012) -> pd.Series:
    """
    Convert amount from INR to USD using the provided conversion rate.
    
    Args:
        amount: Series containing amounts in INR
        conversion_rate: Conversion rate from INR to USD (default: 0.012)
        
    Returns:
        Series containing amounts in USD
    """
    return amount * conversion_rate 
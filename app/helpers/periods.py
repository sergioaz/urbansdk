"""
Period mapping constants and type definitions for time periods.
"""

from typing import Literal

# Period name to number mapping
PERIOD_MAPPING = {
    "Overnight": 1,
    "Early Morning": 2,
    "AM Peak": 3,
    "Midday": 4,
    "Early Afternoon": 5,
    "PM Peak": 6,
    "Evening": 7
}

# Type alias for period names
PeriodName = Literal["Overnight", "Early Morning", "AM Peak", "Midday", "Early Afternoon", "PM Peak", "Evening"]

def get_period_number(period_name: str) -> int:
    """
    Convert period name to period number.
    
    Args:
        period_name: Name of the period
        
    Returns:
        Period number (1-7)
        
    Raises:
        KeyError: If period name is not valid
    """
    return PERIOD_MAPPING[period_name]

def get_period_name(period_number: int) -> str:
    """
    Convert period number to period name.
    
    Args:
        period_number: Period number (1-7)
        
    Returns:
        Period name
        
    Raises:
        ValueError: If period number is not valid
    """
    reverse_mapping = {v: k for k, v in PERIOD_MAPPING.items()}
    if period_number not in reverse_mapping:
        raise ValueError(f"Invalid period number {period_number}. Valid range: 1-7")
    return reverse_mapping[period_number]

def get_valid_periods() -> list[str]:
    """
    Get list of valid period names.
    
    Returns:
        List of valid period names
    """
    return list(PERIOD_MAPPING.keys())

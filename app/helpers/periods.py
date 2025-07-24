"""
Period and day mapping constants and type definitions.
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

# Day of week name to number mapping (Sunday = 1)
DAY_MAPPING = {
    "Sunday": 1,
    "Monday": 2,
    "Tuesday": 3,
    "Wednesday": 4,
    "Thursday": 5,
    "Friday": 6,
    "Saturday": 7
}

# Type aliases
PeriodName = Literal["Overnight", "Early Morning", "AM Peak", "Midday", "Early Afternoon", "PM Peak", "Evening"]
DayName = Literal["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]

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

def get_day_number(day_name: str) -> int:
    """
    Convert day name to day number.
    
    Args:
        day_name: Name of the day
        
    Returns:
        Day number (1-7, where 1=Sunday)
        
    Raises:
        KeyError: If day name is not valid
    """
    return DAY_MAPPING[day_name]

def get_day_name(day_number: int) -> str:
    """
    Convert day number to day name.
    
    Args:
        day_number: Day number (1-7, where 1=Sunday)
        
    Returns:
        Day name
        
    Raises:
        ValueError: If day number is not valid
    """
    reverse_mapping = {v: k for k, v in DAY_MAPPING.items()}
    if day_number not in reverse_mapping:
        raise ValueError(f"Invalid day number {day_number}. Valid range: 1-7")
    return reverse_mapping[day_number]

def get_valid_days() -> list[str]:
    """
    Get list of valid day names.
    
    Returns:
        List of valid day names
    """
    return list(DAY_MAPPING.keys())

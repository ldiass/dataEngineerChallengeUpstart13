def business_days_calc(num_days: int, start_dow: int) -> int:
    """
    Calculates business days between two dates, excluding weekends. Complexity O(n).
    Implementation based in:
    https://stackoverflow.com/questions/43541944/fastest-algorithm-to-calculate-the-number-of-business-days-between-two-dates 

    :param num_days: Total days difference between start and end date.
    :param start_dow: Day of the week of the start date (0 = Monday, 6 = Sunday)
    :return: Number of business days.
    """
    num_holidays = (num_days // 7) * 2  
    
    remaining_days = num_days % 7  

    if start_dow == 1:  # Monday
        num_holidays += min(remaining_days - 4, 2) if remaining_days > 4 else 0
    elif start_dow == 2:  # Tuesday
        num_holidays += min(remaining_days - 3, 2) if remaining_days > 3 else 0
    elif start_dow == 3:  # Wednesday
        num_holidays += min(remaining_days - 2, 2) if remaining_days > 2 else 0
    elif start_dow == 4:  # Thursday
        num_holidays += min(remaining_days - 1, 2) if remaining_days > 1 else 0
    elif start_dow == 5:  # Friday
        num_holidays += min(remaining_days, 2) if remaining_days > 0 else 0
    elif start_dow == 6:  # Saturday
        num_holidays += + (1 if remaining_days > 0 else 0)
    elif start_dow == 0:  # Sunday
        num_holidays += + (1 if remaining_days > 5 else 0)

    return num_days - num_holidays
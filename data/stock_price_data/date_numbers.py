import pandas as pd

class Date_Numbers:
    def __init__(self, base_date='2000-01-01'):
        # Store the base date as a pandas Timestamp for efficient operations
        self.base_date = pd.Timestamp(base_date)

    def date_to_num(self, dates):
        """
        Convert a date or a pandas Series/list of dates to numbers (days since base date).
        """
        # If input is a single date (scalar), handle it separately
        if isinstance(dates, (pd.Timestamp, str)):
            dates = pd.Timestamp(dates)
            days_difference = (dates - self.base_date).days
        else:
            # Otherwise, handle it as a Series of dates for vectorized operations
            dates = pd.to_datetime(dates)
            days_difference = (dates - self.base_date).dt.days
        
        return days_difference

    def num_to_date(self, nums):
        """
        Convert a number or a pandas Series/list of numbers (days since base date) back to dates.
        """
        # Convert to pandas Series if input is a list or scalar number, use to_timedelta for conversion
        if isinstance(nums, (list, pd.Series)):
            return self.base_date + pd.to_timedelta(nums, unit='D')
        else:
            return self.base_date + pd.to_timedelta(nums, unit='D')


if __name__ == "__main__":
    # Create an instance of Date_Numbers
    date_numbers = Date_Numbers()

    # Test with a single date after base date
    single_date = pd.Timestamp('2024-01-01')
    days_diff_single = date_numbers.date_to_num(single_date)
    print(f"Days since base date (single date after base): {days_diff_single}")

    # Test with a single date before base date
    single_date_before = pd.Timestamp('1997-01-01')
    days_diff_single_before = date_numbers.date_to_num(single_date_before)
    print(f"Days since base date (single date before base): {days_diff_single_before}")

    # Test with an ambiguous date (day <= 12, expecting YYYY-MM-DD)
    ambiguous_date = pd.Timestamp('2024-03-12')
    days_diff_ambiguous = date_numbers.date_to_num(ambiguous_date)
    print(f"Days since base date (ambiguous date 2024-03-12): {days_diff_ambiguous}")

    # Test with a series of dates including before base date and ambiguous dates
    dates_series = pd.Series([pd.Timestamp('2024-01-01'), 
                              pd.Timestamp('1997-06-01'),
                              pd.Timestamp('2024-03-12'),
                              pd.Timestamp('2024-12-01'),
                              pd.Timestamp('2000-01-01')])
    days_diff_series = date_numbers.date_to_num(dates_series)
    print(f"Days since base date (series of dates):\n{days_diff_series}")

    # Test with a single negative number of days
    single_num_negative = -1096
    reconstructed_date_single_negative = date_numbers.num_to_date(single_num_negative)
    print(f"Reconstructed date from days (single negative number): {reconstructed_date_single_negative}")

    # Test with a series of numbers including negative numbers
    numbers_series = pd.Series([8766, -1096, 8766 + 71, 8766 + 335])
    reconstructed_dates_series = date_numbers.num_to_date(numbers_series)
    print(f"Reconstructed dates from days (series of numbers):\n{reconstructed_dates_series}")

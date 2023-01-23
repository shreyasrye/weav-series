import pandas as pd
import datetime
import numpy as np

# The values of each frequency in terms of seconds
frequency_multipliers = { 
    'N': 1/1000000000,
    'U': 1/1000000,
    'L': 1/1000,
    'S': 1,
    'T': 60,# minute
    'H': 3600, # hour
    'D': 86400, # day
    'B': 86400,
    'W': 7*86400, # week
    'M': 30.5*7*86400, # month
    'Q': 3*30.5*7*86400,
    'Y': 365.25*86400  # year
}


possible_operations = ['sum', 'mean', 'min', 'max', 'std', 'first', 'last', \
'median', 'var', 'sem', 'skew', 'quantile']


# Checks the validity of operation and frequency inputs, and 
def is_time_series_compatible(dframe, cat_columns):
    if 'd' not in cat_columns.keys():
        return False

    hasContinuousDateColumn, hasNum = False, False
    
    date_columns =  cat_columns['d']

    # check for regular data (i.e. consistent intervals between dates)
    for d in date_columns:
        diffs = dframe[d].diff()
        interval = diffs.value_counts()
        # If there exist more than 7 different intervals, say that the dates aren't applicable
        if len(interval) > 0 and len(interval) < 7:
            hasContinuousDateColumn = True
            break
    # check for irregular data (for example, timestamp data0) 
    # TODO this is set to always true for now, until I find a way to handle irregular data
        else:
            hasContinuousDateColumn = True
            break
    # check for numerical values
    if any(dframe.dtypes == 'int64') or any(dframe.dtypes == 'float64'):
        hasNum = True

    return hasContinuousDateColumn and hasNum
    

# formats the provided column to datetime64
def format_column(dframe, date_col):
    # if date_col is already formatted correctly, then we don't need to do anything
    if dframe[date_col].dtype == 'datetime64[ns]':
        return dframe[date_col]
    
    # case where we are dealing with float or int values
    if dframe[date_col].dtype == 'float64' or dframe[date_col].dtype == 'int64':
        return -1
    temporary_conversion = pd.to_datetime(dframe[date_col], errors='coerce', infer_datetime_format=True)
    percent_datetime = temporary_conversion.notnull().mean()
    if percent_datetime >= 0.9:
        dframe[date_col] = temporary_conversion
        return dframe[date_col]
    return -1

# iterates through each column in the dataframe and 
# adds date columns, value columns and invalid columns to respective lists
def categorize_columns(dframe):
    column_dict = {}

    for col in dframe.columns:
        if dframe[col].dtype == 'float64' or dframe[col].dtype == 'int64':
            if 'v' in column_dict.keys():
                column_dict['v'].append(col)
            else:
                column_dict['v'] = [col]

        elif dframe[col].dtype != 'datetime64[ns]': 
            formatted = format_column(dframe, col)
            if not isinstance(formatted, int):
                if 'd' in column_dict.keys():
                    column_dict['d'].append(col)
                else:
                    column_dict['d'] = [col]
        elif dframe[col].dtype == 'datetime64[ns]': 
            if 'd' in column_dict.keys():
                column_dict['d'].append(col)
            else:
                column_dict['d'] = [col]
        else: # others are miscellaneous
            if 'm' in column_dict.keys():
                column_dict['m'].append(col)
            else:
                column_dict['m'] = [col]
    return column_dict


# Given date columns, return a dictionary of the modes of the intervals of each one
def find_date_intervals(dframe, d_cols: list):
    modes = {}
    for col in d_cols:
        diffs = dframe[col].diff()
        # take the mode of the differences in each column
        modes[col] = diffs.mode()[0]

    return modes
    

# check if the frequency provided is lower than the frequency of the date column
def compare_frequencies(dframe, desired_freq, date_column):
    try:
        f_num = int(desired_freq[0])
    # A ValueError means that no numerical indicator exists
    except ValueError:
        desired_frequency_offset = frequency_multipliers[desired_freq[1:]]
    # separate numerical indicator to multiply the offset by that amount
    else:
        desired_frequency_offset = frequency_multipliers[desired_freq[1:]]*f_num

    # Find the most common difference between dates in date_column in seconds
    date_col_modes = find_date_intervals(dframe,[date_column])
    date_freq_offset = date_col_modes[date_column].total_seconds()

    # Now, both values are converted to seconds, so they can be compared
    return desired_frequency_offset >= date_freq_offset


# Generates a time series
def generate_time_series(dframe, frequency, operation=None, value_columns=[], date_column=None): 
    
    if operation and operation not in possible_operations:
        print("Invalid Operation: ", operation)
        return 
    
    f_num = frequency[0]
    try: 
        f_num = int(f_num)
    except ValueError:
        isolated_frequency = frequency
    else:
        isolated_frequency = frequency[1:]
    if isolated_frequency not in frequency_multipliers.keys():
        print("Invalid frequency: ", frequency)
        return 

    # categorize (and format) the columns of importance
    cats = categorize_columns(dframe)

    if not is_time_series_compatible(dframe, cats):
        print("A time series cannot be created from this data")
        return

    # If not specified, find the date column with the highest frequency
    if not date_column:
        modes = find_date_intervals(dframe, cats['d'])
        date_column = min(modes, key=modes.get)

    # check if the frequency provided is lower than the frequency of the date column 
    if not compare_frequencies(dframe, frequency, date_column):
        print("This frequency is not applicable for this dataframe: pick a lower frequency")
        return  

    # if the set of value columns are given, check if they exist in the dataframe first, then proceed
    if value_columns:
        if not all(col in dframe.columns for col in value_columns):
            print("Value column(s) provided do not exist in the dataframe")
            return
    # otherwise, include all numerical columns
    else:
        value_columns = cats['v']

    # Generate the time series
    dframe.set_index(date_column, inplace=True)

    
    if operation:
        aggs = operation
    else:
        aggs = possible_operations

    resampler = dframe[value_columns].resample(frequency)
    # apply the respective aggregate function(s) to the grouping
    series = resampler.agg(aggs)
    return series


# Returns different properties of an inputted time series # TODO
def analyze(ts):
    return ts.describe()
    # Set the index to the date column
    
# Running

df = pd.read_csv('datasets/pr_transactions.csv')

ts = generate_time_series(df, 'Y', None, ['Amount'])
if ts is not None:
    print(ts)
    # print(analyze(ts))  # uncomment for different format





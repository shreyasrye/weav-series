import pandas as pd
import datetime
import numpy as np
import math


frequencies = { # can have many more options, needs to change #TODO
    'minute': 'T', # minute
    'hour': 'H', # hour
    'day': 'D', # day
    'week': 'W', # week
    'month': 'M', # month
    'year': 'Y'  # year
}


possible_operations = ['sum', 'mean', 'min', 'max', 'std', 'first', 'last', \
'median', 'mad', 'var', 'sem', 'skew', 'kurt', 'quantile']


# If the date column in generate_time_series() is not specified and there are 
# multiple date columns, find and return the one with the highest frequency
def find_best_date_column(d_cols):
    # TODO

    return -1

# Checks the validity of operation and frequency inputs, and 
def is_time_series_compatible(dframe, d_col=None):

    date_columns, hasContinuousDateColumn, hasNum = [], False, False
    
    # if the user has provided a date column, then that will be the only 
    # date column that is checked to have a constant interval
    if d_col:
        d_col = format_column(dframe, d_col)
        date_columns = [dframe[d_col]]
    #otherwise, check all date columns
    else:
        date_columns =  categorize_columns(dframe)['d']

    for d in date_columns:
        diffs = d.diff()
        interval = diffs.value_counts()
        # If there exist more than 3 different intervals, say that the dates aren't applicable
        hasContinuousDateColumn = True if interval > 0 and interval < 3 else False

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
                column_dict['v'].append(dframe[col])
            else:
                column_dict['v'] = [dframe[col]]

        elif dframe[col].dtype != 'datetime64[ns]': 
            formatted = format_column(dframe, col)
            if not isinstance(formatted, int):
                if 'd' in column_dict.keys():
                    column_dict['d'].append(dframe[col])
                else:
                    column_dict['d'] = [dframe[col]]
        else: # others are miscellaneous
            if 'm' in column_dict.keys():
                column_dict['m'].append(dframe[col])
            else:
                column_dict['m'] = [dframe[col]]
    return column_dict
    
# compares the frequency of d_column with freq, and returns true if freq is 
# lower than the frequency of d_column, false otherwise
def compare_frequency(d_column, freq):
    return True # TODO

# frequency inputs:
    #  'day': For a daily time series
    #  'week': For a weekly time series
    #  'month': For a monthly time series
    #  'year': For a yearly time series
# operation inputs:
    # 'sum': for summation of data at every frequency
    # 'average': for the mean of data at every  frequency
def generate_time_series(dframe, frequency=None, operation='sum', value_columns=[], date_column=None): 
    # if the date column is not entered by default, take the first one that exists in dframe

    #TODO : 
    # if no frequency is provided, do all possible frequencies
    # if frequency is provided, make sure that it is lower than the frequency of the dates provided

    if frequency not in frequencies:
        print("Invalid frequency: " + frequency)
        return False
    # if operation not in possible_operations: # need some way to check if custom functions are applicable to resampler objects
    #     print("Invalid operation: " + operation) # TODO
    #     return False

    if not is_time_series_compatible:
        print("A time series cannot be created from this data")
        return

    cats = categorize_columns(dframe) 

    # Find and format date column to base the time series on # TODO find best date column
    if not date_column:
        date_column = find_best_date_column(cats['d'])
    else:
        # check if the frequency provided is lower than the frequency of the date column
        if not compare_frequency(date_column, frequency):
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

    resampler = dframe[value_columns].resample(frequencies[frequency])
    # apply the respective aggregate function(s) to the grouping
    series = resampler.agg(aggs)
    return series


# Returns different properties of an inputted time series # TODO
def analysis(ts):
    return ts.describe()


# Running

df = pd.read_csv('datasets/1979-2021.csv')

ts = generate_time_series(df, 'year', 'sum', ['United States(USD)'])
print(ts)

# Property-based testing TODO


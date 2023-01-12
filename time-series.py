import pandas as pd
import datetime
import numpy as np
import math


frequencies = { # can have many more options, needs to change #TODO
    'T': pd.offsets.Minute,# minute
    'H': pd.offsets.Hour, # hour
    'D': pd.offsets.Day, # day
    'W': pd.offsets.Week, # week
    'M': pd.offsets.MonthBegin, # month
    'Y': pd.offsets.Day(365)  # year
}


possible_operations = ['sum', 'mean', 'min', 'max', 'std', 'first', 'last', \
'median', 'var', 'sem', 'skew', 'quantile']


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
        diffs = dframe[d].diff()
        interval = diffs.value_counts()
        # If there exist more than 3 different intervals, say that the dates aren't applicable
        if len(interval) > 0 and len(interval) < 7:
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
        else: # others are miscellaneous
            if 'm' in column_dict.keys():
                column_dict['m'].append(col)
            else:
                column_dict['m'] = [col]
    return column_dict


 # check if the frequency provided is lower than the frequency of the date column   
def compare_frequencies(dframe, desired_freq, date_column):
    try:
        f_num = int(desired_freq[0])
    # A ValueError means that no numerical indicator exists
    except ValueError:
        desired_frequency_offset = pd.tseries.frequencies.to_offset(desired_freq)
    # separate numerical indicator to multiply the offset by that amount
    else:
        desired_frequency_offset = pd.tseries.frequencies.to_offset(desired_freq)*f_num

    date_column_freq = pd.infer_freq(dframe[date_column])
    if not date_column_freq:
        print("Frequency of the date column is not defined.")
        return
    
    date_col_offset = pd.tseries.frequencies.to_offset(date_column_freq)
    # True if the desired frequency has offsets that are larger than those of the date column
    is_down_sampling = desired_frequency_offset >= date_col_offset
    if not is_down_sampling:
        return False


# finds and returns the frequency of a provided date column
def find_frequency(dc):
    return


def find_offset(freq):
    if not isinstance(freq, str):
        print("Invalid Frequency: ", freq)
        return -1
    # Check if there is a numerical indicator before the frequency type
    try:
        f_num = freq[0]
    # A ValueError means that no numerical indicator exists
    except ValueError:
        return frequencies[freq](1)
    # separate numerical indicator from frequency so that dictionary can be accessed
    freq = freq[1:]
    return frequencies[freq](int(f_num))


# frequency inputs:
    #  'day': For a daily time series
    #  'week': For a weekly time series
    #  'month': For a monthly time series
    #  'year': For a yearly time series
# operation inputs:
    # 'sum': for summation of data at every frequency
    # 'average': for the mean of data at every  frequency
def generate_time_series(dframe, frequency=None, operation=None, value_columns=[], date_column=None): 
    # if the date column is not entered by default, take the first one that exists in dframe

    #TODO : 
    # if no frequency is provided, do all possible frequencies

    # if frequency not in frequencies:
    #     print("Invalid frequency: " + frequency)
    #     return False

    if operation and operation not in possible_operations:
        print("Invalid Operation: ", operation)
        return

    if not is_time_series_compatible(dframe):
        print("A time series cannot be created from this data")
        return

    cats = categorize_columns(dframe) 

    # Find and format date column to base the time series on 
    if not date_column:
        date_column = find_best_date_column(cats['d'])

    # check if the frequency provided is lower than the frequency of the date column TODO
    # if not compare_frequencies(dframe, frequency, date_column):
    #     print("This frequency is not applicable for this dataframe: pick a lower frequency")
    #     return  

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
def analysis(ts):
    return ts.describe()


# Running

df = pd.read_csv('datasets/1979-2021.csv')

ts = generate_time_series(df, '2M', None , ['United States(USD)'], 'Date')
print(ts)


# Property-based testing TODO


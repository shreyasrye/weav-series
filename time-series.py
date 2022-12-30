import pandas as pd
import datetime
from pandas.api.types import is_datetime64_any_dtype as is_datetime
import numpy as np

# formats the provided column to datetime64
def format_column(dframe, date_col):
    # if date_col is already formatted correctly, then we don't need to do anything
    if dframe[date_col].dtype == 'datetime64[ns]':
        return dframe[date_col]
    
    # case where we are dealing with float or int values
    if dframe[date_col].dtype == 'float64' or dframe[date_col].dtype == 'int64':
        return -1
    try:
        dframe[date_col] = pd.to_datetime(dframe[date_col], errors='coerce', infer_datetime_format=True)
        return dframe[date_col]
    except:
        return -1
    
    
# returns whether or not there exists at least one row from dframe can be constructed into a time series
def any_valid(dframe): 
    hasDate, hasNum = False, False
    
    # check for dates
    for col in dframe.columns:
        if dframe[col].dtype == 'datetime64[ns]':
            return True
        if(pd.to_datetime(df[col], errors='coerce', infer_datetime_format=True).notnull().any()):
            return True
        return False

    # check for numerical values
    if any(dframe.dtypes == 'int64') or any(dframe.dtypes == 'float64'):
        hasNum = True

    # return intersection of booleans
    return hasDate and hasNum

# Assuming a dataframe is roughly valid, find_date_column() 
# checks if there exists 1 or more columns in dframe that 
# contains a series of parsable dates which have over 90% non-null 
# values and a chronological series of dates.
# returns: a list of all date columns in dframe, and an empty list if none exist
def find_date_columns(dframe): 
    # store date columns in this list
    d_ls = []

    # A column that has date values stored as datetime64[ns] makes our problem  easy
    for col in dframe.columns:
        if dframe[col].dtype == 'datetime64[ns]': 
            percent_datetime = dframe[col].notnull().mean()
            if percent_datetime >= 0.9: #and (dframe[col].is_monotonic_increasing or dframe[col].is_monotonic_decreasing):
                d_ls.append(col)
                continue
        
        column_values = format_column(dframe, col)
        
        if isinstance(column_values, int):
            continue

        percent_datetime = column_values.notnull().mean()
        if percent_datetime >= 0.9: #and (dframe[col].is_monotonic_increasing or dframe[col].is_monotonic_decreasing):
            d_ls.append(col)
    return d_ls


# group factor can be changed to week, month and year
# group_operation takes 'sum' and 'average'
def time_series(dframe, date_column=None, group_factor='day', group_operation='sum'): 
    # if the date column is not entered by default, take the first one that exists in dframe
    if not any_valid(dframe):
        print("A time series cannot be created from this data")
        return
    if date_column == None:
        cols = find_date_columns(dframe)
        if cols:
            date_column = cols[0]
        else:
            print("A time series cannot be created from this data")
            return
    
    if dframe[date_column].dtype != 'datetime64[ns]':
        format_column(dframe, date_column)

    if group_factor == 'day':
        if group_operation == 'sum':
            return df.groupby(df[date_column].dt.date).sum()
        elif group_operation == 'average':
            return df.groupby(df[date_column].dt.date).mean()
        else:
            print("Invalid group operation")
    elif group_factor == 'week':
        if group_operation == 'sum':
            return df.groupby(df[date_column].dt.week).sum()
        elif group_operation == 'average':
            return df.groupby(df[date_column].dt.week).mean()
        else:
            print("Invalid group operation")
    elif group_factor == 'month':
        if group_operation == 'sum':
            return df.groupby(df[date_column].dt.month).sum()
        elif group_operation == 'average':
            return df.groupby(df[date_column].dt.month).mean()
        else:
            print("Invalid group operation")
    elif group_factor == 'year':
        if group_operation == 'sum':
            return df.groupby(df[date_column].dt.year).sum()
        elif group_operation == 'average':
            return df.groupby(df[date_column].dt.year).mean()
        else:
            print("Invalid group operation")
    else:
        print("Invalid group factor")


# testing
df = pd.read_csv('datasets/PowerGeneration.csv')

# print(df.dtypes)  
print(time_series(df, None, 'week', 'average'))


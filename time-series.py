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

def groupby(dframe, group_factor): # group by date, month, year, week
    # Current Pseudocode:
        # - check rough validity first
        # - if roughly valid, find a date column (and simultaneously determine if fully valid)
        # - if a column i s found, preprocess data (if needed) and then create a time series by grouping
    return None


# testing
df = pd.read_csv('datasets/1979-2021.csv')

# print(df.dtypes)  
print(find_date_columns(df))


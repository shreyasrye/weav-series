import pandas as pd
import datetime
from pandas.api.types import is_datetime64_any_dtype as is_datetime
import numpy as np


def format_column(dframe, date_col):
    try:
        dframe[date_col] = pd.to_datetime(dframe[date_col], infer_datetime_format=True)
        return 1
    except:
        print("This column cannot be formatted to the type: 'datetime64'")
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

# Assuming a dataframe is roughly valid, find_date_column() checks if there exists a column in dframe that 
# contains a series of parsable dates which have over 90% non-null values and a chronological series of dates.
def find_date_column(dframe):
    return None

def groupby(dframe, group_factor): # group by date, month, year, week
    # Current Pseudocode:
        # - check rough validity first
        # - if roughly valid, find a date column (and simultaneously determine if fully valid)
        # - if a column is found, preprocess data (if needed) and then create a time series by grouping
    return None


# testing

df = pd.read_csv('datasets/christmas_movies.csv')
print(any_valid(df))


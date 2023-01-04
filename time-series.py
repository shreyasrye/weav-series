import pandas as pd
import datetime
import numpy as np

    
# returns whether or not there exists at least one row from dframe can be constructed into a time series
def any_valid(dframe): 
    hasDate, hasNum = False, False
    
    # check for dates
    for col in dframe.columns:
        if dframe[col].dtype == 'datetime64[ns]':
            return True
        if pd.to_datetime(df[col], errors='coerce', infer_datetime_format=True).notnull().any():
            return True
        return False

    # check for numerical values
    if any(dframe.dtypes == 'int64') or any(dframe.dtypes == 'float64'):
        hasNum = True

    # return intersection of booleans
    return hasDate and hasNum


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
    d_ls, vals, inv = [], [], []

    for col in dframe.columns:
        percent_valid = dframe[col].notnull().mean()
        if percent_valid < 0.5:
            inv.append(col)
            continue
        elif dframe[col].dtype == 'float64' or dframe[col].dtype == 'int64':
            vals.append(col)
        elif dframe[col].dtype != 'datetime64[ns]': 
            formatted = format_column(dframe, col)
            if not isinstance(formatted, int):
                dframe[col] = formatted
                d_ls.append(col)
        
    return (d_ls, vals, inv)

def reformat_date(dframe, date_col):
    dframe["Month and Year"] = df[date_col].dt.strftime("%b %Y")
    return dframe


# frequency inputs:
    #  'D': For a daily time series
    #  'W': For a weekly time series
    #  'M': For a monthly time series
    #  'Y': For a yearly time series
# operation inputs:
    # 'sum': for summation of data at every frequency
    # 'average': for the mean of data at every  frequency
def time_series(dframe, frequency=None, operation='sum', value_columns=[], date_column=None): 
    # if the date column is not entered by default, take the first one that exists in dframe
    if not any_valid(dframe):
        print("A time series cannot be created from this data")
        return

    cats = categorize_columns(dframe)

    # Find and format date column to base the time series on
    if date_column == None:
        cols = cats[0]
        if cols:
            date_column = cols[0]
        else:
            print("A time series cannot be created from this data: there are no date columns")
            return

    if not value_columns:
        value_columns = cats[1]

    # Generate the time series
    dframe.set_index(date_column, inplace=True)

    try:
        if operation == 'sum':
            series = dframe[value_columns].resample(frequency).sum()
        elif operation == 'average':
            series = dframe[value_columns].resample(frequency).mean()
        else:
            print("Invalid operation provided: " + operation)
    except ValueError:
        print("Invalid frequency: " + frequency)

    return series

# Returns different properties of an inputted time series
def analysis(ts):
    return ts.describe()


# testing

df = pd.read_csv('datasets/1979-2021.csv')

ts = time_series(df, 'W', 'sum', ['United States(USD)'])
print(ts)


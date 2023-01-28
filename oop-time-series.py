import pandas as pd
import datetime
import numpy as np
import math
from collections import OrderedDict
from statsmodels.tsa.seasonal import seasonal_decompose
import matplotlib.pyplot as plt


class TimeSeries():
    
    # Declaration and initialization of attributes
    dframe, time_series, frequency, agg_operations, date_column, date_column_frequency, decomposition = None, None, None, None, None, None, None
    value_columns = []
    is_irregular, is_time_series = False, False
    interpolation_method = 'linear'
    validity_threshold = 0.4

    frequency_multipliers = OrderedDict() # stores the intervals between units of each frequency in terms of seconds
    frequency_multipliers['N'] = 1/1000000000
    frequency_multipliers['U'] = 1/1000000
    frequency_multipliers['L'] = 1/1000
    frequency_multipliers['S'] = 1
    frequency_multipliers['T'] = 60# minute
    frequency_multipliers['H'] = 3600 # hour
    frequency_multipliers['D'] = 86400, # day
    frequency_multipliers['B'] = 86400,
    frequency_multipliers['W'] = 7*86400, # week
    frequency_multipliers['M'] = 30.5*7*86400, # month
    frequency_multipliers['Q'] = 3*30.5*7*86400,
    frequency_multipliers['Y'] = 365.25*86400  # year
    

    possible_operations = ['sum', 'mean', 'min', 'max', 'std', 'first', 'last', \
    'median', 'var', 'sem', 'skew', 'quantile']


    def __init__(self, dataframe, frequency: str, operation=None, value_columns=[], date_column=None):
        self.dframe = dataframe

        # Preliminary checks for the provided desired frequency and operation(s)
        if frequency:
            self.frequency = frequency
        f_num = frequency[0] 
        try: 
            f_num = int(f_num)
        except ValueError:
            isolated_frequency = frequency
        else:
            isolated_frequency = frequency[1:]
        if isolated_frequency not in self.frequency_multipliers.keys():
            print("Invalid frequency")
            return 

        if operation:
            self.agg_operations = operation
        elif operation and operation not in self.possible_operations:
            print("Invalid Operation")
            return
        else:
            self.agg_operations = self.possible_operations

        # categorize (and format) the columns of importance
        categories = self.categorize_columns(self.dframe)

        # checks whether the input dataframe contains at least 1 numerical column and 
        # 1 date column where the dates are either a continuous series or irregular  
        if not self.is_compatible(self.dframe, categories):
            print("A time series cannot be created from this data") # TODO specify
            return
        
        # If not specified, find the date column with the highest frequency
        if date_column:
            # Check to see if the provided column exists in the dataframe
            if date_column not in categories['d']:
                print("Invalid Date column provided")
                return
            self.date_column = date_column   
        else:    
            modes = self.find_date_intervals(self.dframe, categories['d']) # TODO could do this in is_compatible?
            self.date_column = min(modes, key=modes.get)

        # check if the frequency provided is lower than the frequency of the date column 
        if not self.compare_frequencies(self.dframe, self.frequency, self.date_column): # TODO if not provided, set frequency to natural freq
            print("This frequency is not applicable for this dataframe: pick a lower frequency")
            return

        # if the set of value columns are given, check if they exist in the dataframe first, then proceed
        if value_columns:
            if not all(col in self.dframe.columns for col in value_columns):
                print("Value column(s) provided do not exist in the dataframe")
                return
            self.value_columns = value_columns
        # otherwise, include all numerical columns
        else:
            self.value_columns = categories['v']

    # Resample the dataframe once preliminary checks are over
        # Generate the time series
        self.dframe.set_index(self.date_column, inplace=True)

        resampler = self.dframe[self.value_columns].resample(self.frequency)
        # apply the respective aggregate function(s) to the grouping
        self.time_series = resampler.agg(self.agg_operations)
        
        #interpolate missing values 
        for vc in self.value_columns:
            self.time_series[vc] = self.time_series[vc].interpolate(method=self.interpolation_method)

        plt.plot(self.time_series)

        # perform the decomposition for analytics
        self.decomposition = seasonal_decompose(self.time_series[self.value_columns], model='multiplicable')


    # Displays the generated time series
    def display(self): 
        # print out the dataframe of the time series
        print(self.time_series)

        # Print out the information about the time series
        print()
        print(f"Frequency: {self.frequency}")
        print(f"Value Columns: {self.value_columns}")
        print(f"Trend: {self.decomposition.trend}")
        print(f"Seasonality: {self.decomposition.seasonal}")
        print(f"Residual: {self.decomposition.resid}")

        plt.show()



    # Given date columns, return a dictionary of the modes of the intervals of each one
    def find_date_intervals(self, dframe, d_cols: list):
        modes = {}
        for col in d_cols:
            diffs = dframe[col].diff()
            # take the mode of the differences in each column
            modes[col] = diffs.mode()[0]

        return modes

    
    # check if the frequency provided is lower than the frequency of the date column
    def compare_frequencies(self, dframe, desired_freq, date_column):
        try:
            f_num = int(desired_freq[0])
        # A ValueError means that no numerical indicator exists
        except ValueError:
            desired_frequency_offset = self.frequency_multipliers[desired_freq]
        # separate numerical indicator to multiply the offset by that amount
        else:
            desired_frequency_offset = self.frequency_multipliers[desired_freq[1:]]*f_num

        # Find the most common difference between dates in date_column in seconds
        date_col_mode = self.find_date_intervals(dframe,[date_column])
        date_freq_offset = date_col_mode[date_column].total_seconds()

        # Now, both values are converted to seconds, so they can be compared
        return desired_frequency_offset >= date_freq_offset
    
    # Checks the validity of operation and frequency inputs, and 
    def is_compatible(self, dframe, cat_columns):
        if 'd' not in cat_columns.keys():
            return False

        hasContinuousDateColumn, hasNum = False, False
        
        date_columns = cat_columns['d']

        # check for regular data (i.e. consistent intervals between dates)
        for d in date_columns:
            diffs = dframe[d].diff()
            interval = diffs.value_counts()
            # If there exist more than 7 different intervals, say that the dates aren't applicable
            if len(interval) > 0 and len(interval) < 7: # TODO check if arrow has a way to do this
                hasContinuousDateColumn = True
                break
        # check for irregular data (for example, timestamp data) 
            else:
                self.is_irregular = True
        # check for numerical values
        if any(dframe.dtypes == 'int64') or any(dframe.dtypes == 'float64'):
            hasNum = True

        return (hasContinuousDateColumn and hasNum) or (self.is_irregular and hasNum)


    # If the dates are irregular, then we downsample them until they become regular
    def down_sample_to_regular(self, dframe, date_col, freq_dict):
        average_interval = dframe[date_col].diff().dt.total_seconds().mean()
        return 


    # formats the provided column to datetime64
    def format_column(self, dframe, date_col): # change name
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
    def categorize_columns(self, dframe):
        column_dict = {}

        for col in dframe.columns:

            percent_null = dframe[col].isnull().mean()
            percent_zero = dframe[col].eq(0).mean()
            percent_empty = dframe[col].eq('').mean()
            # Checks whether any of the percentages above fall over the threshold, in which case they will be disregarded
            if max(percent_null, percent_zero, percent_empty) > self.validity_threshold:
                if 'u' in column_dict.keys():
                    column_dict['u'].append(col)
                else:
                    column_dict['u'] = [col]
            elif dframe[col].dtype == 'float64' or dframe[col].dtype == 'int64':
                if 'v' in column_dict.keys():
                    column_dict['v'].append(col)
                else:
                    column_dict['v'] = [col]

            elif dframe[col].dtype != 'datetime64[ns]': 
                formatted = self.format_column(dframe, col)
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

    

df = pd.read_csv('datasets/1979-2021.csv')
ts = TimeSeries(df, '3Y', 'mean', ['United States(USD)'])
ts.display()
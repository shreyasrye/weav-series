import pandas as pd
import datetime
import numpy as np



class TimeSeriesData(pd.DataFrame) :

    date_frequency = None
    is_irregular = False

    def __init__(self, dataframe):
        super().__init__(dataframe)

        
class TimeSeries:
    
    # Fields
    dframe, time_series, frequency, agg_operations, date_column = None, None, None, None, None
    value_columns = []
    is_irregular = False
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
        cats = self.categorize_columns(self.dframe)

        # checks whether the input dataframe contains at least 1 numerical column and 
        # 1 date column where the dates are either a continuous series or irregular  
        if not self.is_compatible(self.dframe, cats):
            print("A time series cannot be created from this data")
            return
        
        # If not specified, find the date column with the highest frequency
        if date_column:
            # Check to see if the provided column exists in the dataframe
            if date_column not in cats['d']:
                print("Invalid Date column provided")
                return
            self.date_column = date_column   
        else:    
            modes = self.find_date_intervals(self.dframe, cats['d'])
            self.date_column = min(modes, key=modes.get)

        # check if the frequency provided is lower than the frequency of the date column 
        if not self.compare_frequencies(self.dframe, self.frequency, self.date_column):
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
            self.value_columns = cats['v']

        # Resample the dataframe once preliminary checks are over

        # Generate the time series
        self.dframe.set_index(self.date_column, inplace=True)

        resampler = self.dframe[self.value_columns].resample(self.frequency)
        # apply the respective aggregate function(s) to the grouping
        self.time_series = resampler.agg(self.agg_operations)


    # Displays the generated time series
    def display(self): 
        return self.time_series


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
            desired_frequency_offset = self.frequency_multipliers[desired_freq[1:]]
        # separate numerical indicator to multiply the offset by that amount
        else:
            desired_frequency_offset = self.frequency_multipliers[desired_freq[1:]]*f_num

        # Find the most common difference between dates in date_column in seconds
        date_col_modes = self.find_date_intervals(dframe,[date_column])
        date_freq_offset = date_col_modes[date_column].total_seconds()

        # Now, both values are converted to seconds, so they can be compared
        return desired_frequency_offset >= date_freq_offset
    
    # Checks the validity of operation and frequency inputs, and 
    def is_compatible(self, dframe, cat_columns):
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
    def format_column(self, dframe, date_col):
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
            if dframe[col].dtype == 'float64' or dframe[col].dtype == 'int64':
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

    

df = pd.read_csv('datasets/pr_transactions.csv')
ts = TimeSeries(df, '5D', None, ['Amount'])
print(ts.display())
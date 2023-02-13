# Weav AI: Time Series Data Handling and Generation
This page provides a simple run-down of a class written for an implementation of a time series using Pandas for the [Weav AI platform](https://weav.ai/). The provided code is for a `TimeSeries` class that is capable of handling and generating time series data. 

`oop-time-series.py` is the primary implementation of the time series that is object-oriented in nature. There also exists an alternative implementation (as a series of stand-alone functions): `time-series.py`.

## Prerequisites

The following libraries are required to run the code:
 - Pandas
 - Numpy
 - Matplotlib
 - Statsmodels

Additionally, this code was written in a conda environment, and I'll provide the specific versions of the above libraries that I used in said environment when writing this code:

## Initialization
The class takes in 3 main arguments when initializing an object:
- dataframe: the main dataframe to be processed
- frequency: the desired frequency of the time series data. Should be in the format [integer][frequency unit (N/U/L/S/T/H/D/B/W/M/Q/Y)]
- operation: (optional) a string or list of strings representing the aggregate operations to be performed. If not provided, all of the available operations will be performed. Available operations are sum, mean, min, max, std, first, last, median, var, sem, skew, and quantile
- value_columns: (optional) a list of column names to be used as value columns in the time series. If not provided, all numerical columns in the dataframe will be used.
- date_column: (optional) the name of the date column to be used as the index in the time series. If not provided, the date column with the highest frequency in the dataframe will be used.

## Features

The code is designed such that a user will provide a dataset and the functions will perform the following:
 - Deduce whether or not the given data is a time series
 - Deduce whether or not the given data is able to be turned into a time series
    - Checks are made to ensure that there exists at least one column of data that contains numerical values that the time series can resample/aggregate on
    - Secondly, checks are made to ensure that there exists at least one column that contains values of type 'datetime64' (implying a column of dates) or at least one column of objects or strings that can be converted to 'datetime64'.
 - Filter and transform the data as necessary.
 - Plot the time series data in various forms, including line plots, bar plots, and histograms.
 - Calculate various summary statistics, such as mean, median, and standard deviation.

## Additional Details
- The compare_frequencies method returns True if the frequency provided is lower than the frequency of the date column and False otherwise.
- The categorize_columns method categorizes the columns of the dataframe into 3 categories: numerical, date, and others.
- The find_date_intervals method determines the frequency of the date column by finding the mode of the intervals between consecutive dates in the date column.
- The is_compatible method returns True if the dataframe contains at least 1 numerical column and 1 date column and False otherwise.



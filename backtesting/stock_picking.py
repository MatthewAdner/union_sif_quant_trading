import pandas as pd
import warnings
import numpy as np

def best_on_date(stock_data:pd.DataFrame,date:str,metric:str,abs_val:bool=True,how_many:int=1, max_or_min = 'max',exclude_extremes=True, min_trading_volume=10000, min_trading_volume_value=100000, size_categories=['mega','large','mid','small','micro'], min_market_cap=0):
    """gets the ticker(s) of the best stock or stocks on a specific
    date according to the numerical value of either a specific metric,
    or the absolute value of a specific metric. If there is not enough tickers with data then the
    return value will be None and a warning will be printed and logged

    Args:
        stock_data (pd.DataFrame): the dataframe to use for reference
        date (str): date in the format of 'YYYY-MM-DD'
        metric (str): the name of the column to be used for evaluation
        abs_val (bool, optional): do you want the highest values in the column, or the values furthest from 0. Defaults to True.
        how_many (int, optional): top __ ticker(s). Defaults to 1.
    """

    # takes a slice of dataframe for current date with .xs(date)
    # unstack if because .xs puts it in long format
    # transpose so tickers are the index column
    date_slice = stock_data.xs(date).unstack().transpose()


    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
        warnings.simplefilter("ignore", category=FutureWarning)
        date_slice[metric] = pd.to_numeric(date_slice[metric], errors='coerce')


    # simplify to just relevant data and drop na columns so we don't return useless data
    date_slice = date_slice[date_slice['Market_Cap']>min_market_cap]
    date_slice = date_slice[date_slice['Size_Category'].isin(size_categories)]
    date_slice = date_slice[date_slice['Volume']>min_trading_volume]
    date_slice = date_slice[date_slice['Volume_Value']>min_trading_volume_value]
    date_slice = date_slice.filter(items=['Adj_Close', 'Volume', metric]).dropna()
    date_slice = date_slice[np.abs(date_slice[metric])<50]
    
       

    # if there is not enough data to return the number of tickers wanted, returns 
    if len(date_slice.index)<how_many:
        warningString = f"Warning: not enough data on {date} to run best_on_date. There is only {len(date_slice.index)} non-NaN values in {metric} but you want the top {how_many}. Returning {None}"
        warnings.warn(warningString)
        return(None)
    else:
        if max_or_min =='max':
            if(abs_val):
                # gets the id (Tickers) of the 2 highest absolute value for price_diff_var
                max_tickers = abs(date_slice).nlargest(how_many,[metric]).index.tolist()
            else:
                # gets the id (Tickers) of the 2 highest value for price_diff_var
                max_tickers = date_slice.nlargest(how_many,[metric]).index.tolist()
            return(max_tickers)
        if max_or_min == 'min':
            if(abs_val):
                # gets the id (Tickers) of the 2 highest absolute value for price_diff_var
                min_tickers = abs(date_slice).nsmallest(how_many,[metric]).index.tolist()
            else:
                # gets the id (Tickers) of the 2 highest value for price_diff_var
                min_tickers = date_slice.nsmallest(how_many,[metric]).index.tolist()
            return(min_tickers)

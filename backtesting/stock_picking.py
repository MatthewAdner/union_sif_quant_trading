import pandas as pd
import warnings
import numpy as np

def best_on_date(stock_data,date:str,metric:str,date_already_xs_unstack_transposed=False,abs_val:bool=True,how_many:int=1, max_or_min = 'max',exclude_extremes=True, extreme_filter_value=50, min_trading_volume=10000, min_trading_volume_value=10000, size_categories=['mega','large','mid','small','micro'], min_market_cap=1, avoid_sectors_filter=[], avoid_industries_filter=[], avoid_countries_filter=[], company_data_getter_obj=None):
    """gets the ticker(s) of the best stock or stocks on a specific
    date according to the numerical value of either a specific metric,
    or the absolute value of a specific metric. If there is not enough tickers with data then the
    return value will be None and a warning will be printed and logged

    Args:
    stock_data (pd.DataFrame): the dataframe to use for reference
        date (str): date in the format of 'YYYY-MM-DD'
        metric (str): the name of the column to be used for evaluation
        abs_val (bool, optional): do you want the highest/lowest values or values furthest from/closest to zero. Defaults to True.
        how_many (int, optional): top __ ticker(s). Defaults to 1.
        max_or_min (str, optional): do you want the largest or smallest values. Defaults to 'max'.
        exclude_extremes (bool, optional): exclude values where the metric is greater than exteme_filter_value. Defaults to True.
        extreme_filter_value (int, optional): if you are excluding extremes, exclude values above this values. Defaults to 50.
        min_trading_volume (int, optional): minimum tading volume (in shares) you're willing to invest in. Defaults to 10000.
        min_trading_volume_value (int, optional): minimum trading volume (in dollars) you're willing to invest in. Defaults to 10000.
        size_categories (list, optional): list of size categories you want to look for investments in . Defaults to ['mega','large','mid','small','micro'].
        min_market_cap (int, optional): smallest market cap company you're willing to invest in. Defaults to 1.
        avoid_sectors_filter (list, optional): list of strings representing any sectors to avoid, eg ['Miscellaneous', 'Finance', 'Energy']. Defaults to [].
        avoid_industries_filter (list, optional) list of strings representing any industries to avoid, eg ['Blank Checks', 'Real Estate', 'Restaurants']. Defaults to [].
        avoid_countries_filter (list, optional) list of strings representing any countries to avoid, eg ['Bermuda', 'China']. Defaults to [].
        company_data_getter_obj (obj, optional): pass in company data getter object if you're filtering by something company attributes eg. sector or country. Defaults to None.
    """
    
    

    # takes a slice of dataframe for current date with .xs(date)
    # unstack if because .xs puts it in long format
    # transpose so tickers are the index column
    if date_already_xs_unstack_transposed:
        date_slice = stock_data.copy()
    else:
        date_slice = stock_data.xs(date).unstack().transpose()


    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
        warnings.simplefilter("ignore", category=FutureWarning)
        date_slice[metric] = pd.to_numeric(date_slice[metric], errors='coerce')

    # exclude extremes if desired
    if exclude_extremes:
        date_slice = date_slice[np.abs(date_slice[metric])<extreme_filter_value]
    
    # simplify to just relevant data and drop na columns so we don't return useless data
    date_slice = date_slice[date_slice['Market_Cap']>min_market_cap]
    date_slice = date_slice[date_slice['Size_Category'].isin(size_categories)]
    date_slice = date_slice[date_slice['Volume']>min_trading_volume]
    date_slice = date_slice[date_slice['Volume_Value']>min_trading_volume_value]
    date_slice = date_slice.filter(items=['Adj_Close', 'Volume', metric]).dropna()
    
    #if we are filtering sectors
    if avoid_sectors_filter != []:
        # select rows of date_slice
        date_slice = date_slice.loc[
            #where the sector is not (~ indicates not)
            ~company_data_getter_obj.get_sector(
                date_slice.index
            # in the list of sectors to avoid, or have missing sector data    
            ).isin(avoid_sectors_filter+[np.nan])
        ]
        # company_data_getter_obj.
    
    #if we are filtering industries
    if avoid_industries_filter != []:
        date_slice = date_slice.loc[
            ~company_data_getter_obj.get_industry(
                date_slice.index
            ).isin(avoid_industries_filter+[np.nan])
        ]

    #if we are filtering countries
    if avoid_countries_filter != []:
        date_slice = date_slice.loc[
            ~company_data_getter_obj.get_country(
                date_slice.index
            ).isin(avoid_countries_filter+[np.nan])
        ]

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

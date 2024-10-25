import pandas as pd
import numpy as np

def select_data_subset(input_dataframe, std_dev_day_range='all', reg_day_range='all', ticker_subset='all', price_vars_to_exclude='none', start_date='none'):
    """
    Selects a subset of stock data based on a variety of factors.

    Args:
        input_dataframe (pandas dataframe, required): DataFrame with stock data.
        std_dev_day_range (str, list, optional): Leave as 'all' to include everything, give an int or a list of ints to select only std_devs over that/those periods. Defaults to 'all'.
        reg_day_range (list, optional): Leave as 'all' to include everything, give an int or a list of ints to select only reg values (intercept and coeffs) over that/those periods. Defaults to 'all'.
        ticker_subset (list, optional): Leave as 'all' to include all tickers, or give a list of ticker(s) to keep. Defaults to 'all'.
        price_vars_to_exclude (list, optional): Leave as 'none' to include all price vars, or give a list of price variables to exclude. Defaults to 'none'.
        start_date (str, optional): The date before which you don't want data, in the format 'YYYY-MM-DD'. Defaults to 'none'.

    Returns:
        pandas dataframe: Filtered DataFrame based on the specified criteria.
    """

    # filter date
    if start_date != 'none':
        return_df = input_dataframe[input_dataframe.index > start_date]
    else:
        return_df = input_dataframe
    
    return_cols = list(return_df.columns)

    # filter tickers
    if ticker_subset != 'all':
        # print(return_cols)
        return_cols = list(x for x in return_cols if x[1] in (ticker_subset+['']) )
    
    # exclude price vars
    if price_vars_to_exclude != 'none':
        return_cols = list({x for x in return_cols if x[0] not in price_vars_to_exclude})
    
    # filter std dev day ranges
    if std_dev_day_range != 'all':
        std_dev_day_range = tuple(str(x) for x in std_dev_day_range)
        return_cols = list(
            set(return_cols)
            - set( list(x for x in return_cols if ( x[0].startswith('Std_Dev') and (not (x[0].endswith(std_dev_day_range))) )) )
            )


    # filter regression day ranges
    if reg_day_range != 'all':
        print()
        bad_reg_cols = [
            x for x in return_cols 
            if (x[0].split('_')[0] == 'Intercept' or 
                (len(x[0].split('_')) > 1 and x[0].split('_')[-2] == 'Coeff') )
                ]
        # print(bad_reg_cols)
        # bad_reg_cols = {x for x in bad_reg_cols if any(a in x for a in reg_day_range)}
        # print({x[0] for x in bad_reg_cols})
        # print(bad_reg_cols[0:10])
        bad_reg_cols = {x for x in bad_reg_cols if not any(str(a) in x[0] for a in reg_day_range)}
        # print({x[0] for x in bad_reg_cols})
        return_cols = list(set(return_cols) - set(bad_reg_cols))

    #     reg_day_range = tuple(str(x) for x in reg_day_range)
    #     print(return_cols[-10:])
    #     return_cols = list(
    #         set(return_cols)
    #         - set( x for x in return_cols if 
    #             #   ((x[0].startswith('Intercept_') or x[0].split('_')[-2]=='Coeff') and (not x[0].endswith(reg_day_range))))
    #               ((x[0].split('_')[0]=='Intercept' or x[0].split('_')[-2]=='Coeff') and (not x[0].endswith(reg_day_range))))
    #     )

    # filter regression day ranges
    # if reg_day_range != 'all':
    #     reg_day_range = tuple(str(x) for x in reg_day_range)
    #     print(return_cols[-10:])
    #     return_cols = list(
    #         set(return_cols)
    #         - set( x for x in return_cols if 
    #             #   ((x[0].startswith('Intercept_') or x[0].split('_')[-2]=='Coeff') and (not x[0].endswith(reg_day_range))))
    #               ((x[0].split('_')[0]=='Intercept' or x[0].split('_')[-2]=='Coeff') and (not x[0].endswith(reg_day_range))))
    #     )
    
    if not((ticker_subset == 'all') and (price_vars_to_exclude =='none') and (std_dev_day_range=='all') and (reg_day_range=='all')):
        return_df = return_df[return_cols]

    return(return_df)





def add_lin_reg_prediction(df, reg_range, predict_target_day=0, new_multiindex_col_name=None):
    """adds a linear regression prediction using existing coefficients and intercepts

    Args:
        df (dataframe, required): the dataframe that holds the data
        reg_range (int, required): one of the ranges that regressions exist over in the dataframe
        predict_target (int, optional): the target of the prediction relative to the column. 0 will be that day, 5 will be 5 trading days into the future. Defaults to 0.
        new_multiindex_col_name (str, optional): add a string you want as the multi index title in index 0 of the column name, or leave as None to return non-multi index dataframe. Defaults to None.
    
    Returns:
        pandas dataframe with only the regression prediction values
    """

    # the column that holds the intercept for the regression
    intercept_col = 'Intercept_'+str(reg_range)

    # a list of columns with the coefficients
    coeff_cols = list({x[0] for x in df.columns if(
        x[0].endswith(str(reg_range)) and
        x[0].split('_')[-2]=='Coeff'
    )})

    # a list of columns with what the coeffients are to be multiplied by
    predictor_cols = list({ ('_'.join(x.split('_')[0:-2])) for x in coeff_cols})

    coeff_cols_dict = {}
    for coeff in coeff_cols:
        # get predictor name
        predictor_name = '_'.join(coeff.split('_')[:-2])
        
        # checks if there is an error in the naming system- better to throw an error than have wrong data
        if predictor_name in predictor_cols:
            coeff_cols_dict[predictor_name] = coeff
        else: 
            exit("column names are fucked up, a predictor_col doesn't")
    
    del(coeff_cols)

    # intercept
    return_df = df.xs(intercept_col, axis=1,level=0)

    # plus coefficient* value for each coeficient value pair (for multivar regressions)
    for p_col in predictor_cols:
        return_df = return_df + (df.xs(p_col,axis=1,level=0).values *df.xs(coeff_cols_dict[p_col], axis=1, level =0))
    
    if new_multiindex_col_name == None:
        return(return_df.apply(pd.to_numeric, errors='coerce'))
    else: 
        new_columns = pd.MultiIndex.from_product([[new_multiindex_col_name], return_df.columns])
        return_df.columns= new_columns
        return(return_df.apply(pd.to_numeric, errors='coerce'))
    




def add_price_diff_metric(df, actual_val_col, theo_val_col, std_dev_col, new_multiindex_col_name=None):
    """returns dataframe with difference between theoretical value and actual price in terms of a value

    Args:
        df (pandas dataframe): source dataframe
        actual_val_col (str): the name of the column of the actual price value eg 'Adj_Close'"date_numbers copy.py"
        theo_val_vol (str): the name of the column of the predicted theoretical price 
        std_dev_col (str): the name of the column of standard deviation you want the difference in terms of
        new_multiindex_col_name (str, optional): add a string you want as the multi index title in index 0 of the column name, or leave as None to return non-multi index dataframe. Defaults to None.

    """
    

    return_df = df.xs(theo_val_col, axis=1, level=0)
    return_df = return_df - df.xs(actual_val_col, axis=1, level=0)
    return_df = return_df/ df.xs(std_dev_col, axis=1, level=0)

    if new_multiindex_col_name == None:
        return(return_df)
    else: 
        new_columns = pd.MultiIndex.from_product([[new_multiindex_col_name], return_df.columns])
        return_df.columns = new_columns
        return(return_df)


# class Company_Data_Getter:
#     def __init__(self, company_data, stock_data):
#         self.company_data = company_data
#         self.stock_data = stock_data
        
#     def get_sector(self, tickers):
#         return tickers.map(lambda ticker: self.company_data.at[ticker, 'Sector'])

#     def get_industry(self, tickers):
#         return tickers.map(lambda ticker: self.company_data.at[ticker, 'Industry'])

#     def get_country(self, tickers):
#         return tickers.map(lambda ticker: self.company_data.at[ticker, 'Country'])

#     def get_ipo_year(self, tickers):
#         return tickers.map(lambda ticker: self.company_data.at[ticker, 'IPO_Year'])

#     def get_exchange(self, tickers):
#         return tickers.map(lambda ticker: self.company_data.at[ticker, 'Exchange'])

#     def get_size_category(self, tickers, dates):
#         return [self.stock_data.at[date, ('Size_Category', ticker)] for ticker, date in zip(tickers, dates)]

#     def get_market_cap(self, tickers, dates):
#         return [self.stock_data.at[date, ('Market_Cap', ticker)] for ticker, date in zip(tickers, dates)]


# class Company_Data_Getter:
#     def __init__(self, company_data, stock_data):
#         self.company_data = company_data
#         self.stock_data = stock_data
        
#     def get_sector(self, tickers):
#         return tickers.map(lambda ticker: self.company_data.get(ticker, {}).get('Sector', np.nan))

#     def get_industry(self, tickers):
#         return tickers.map(lambda ticker: self.company_data.get(ticker, {}).get('Industry', np.nan))

#     def get_country(self, tickers):
#         return tickers.map(lambda ticker: self.company_data.get(ticker, {}).get('Country', np.nan))

#     def get_ipo_year(self, tickers):
#         return tickers.map(lambda ticker: self.company_data.get(ticker, {}).get('IPO_Year', np.nan))

#     def get_exchange(self, tickers):
#         return tickers.map(lambda ticker: self.company_data.get(ticker, {}).get('Exchange', np.nan))

#     def get_size_category(self, tickers, dates):
#         return [self.stock_data.get(date, {}).get(('Size_Category', ticker), np.nan) for ticker, date in zip(tickers, dates)]

#     def get_market_cap(self, tickers, dates):
#         return [self.stock_data.get(date, {}).get(('Market_Cap', ticker), np.nan) for ticker, date in zip(tickers, dates)]
    



    import numpy as np  # For NaN values

class Company_Data_Getter:
    def __init__(self, company_data, stock_data):
        self.company_data = company_data
        self.stock_data = stock_data
        
    def get_sector(self, tickers):
        return tickers.map(lambda ticker: self._safe_get_company_data(ticker, 'Sector'))

    def get_industry(self, tickers):
        return tickers.map(lambda ticker: self._safe_get_company_data(ticker, 'Industry'))

    def get_country(self, tickers):
        return tickers.map(lambda ticker: self._safe_get_company_data(ticker, 'Country'))

    def get_ipo_year(self, tickers):
        return tickers.map(lambda ticker: self._safe_get_company_data(ticker, 'IPO_Year'))

    def get_exchange(self, tickers):
        return tickers.map(lambda ticker: self._safe_get_company_data(ticker, 'Exchange'))

    def get_size_category(self, tickers, dates):
        return [self._safe_get_stock_data(date, ('Size_Category', ticker)) for ticker, date in zip(tickers, dates)]

    def get_market_cap(self, tickers, dates):
        return [self._safe_get_stock_data(date, ('Market_Cap', ticker)) for ticker, date in zip(tickers, dates)]
    
    def _safe_get_company_data(self, ticker, column):
        try:
            return self.company_data.at[ticker, column]
        except (KeyError, IndexError):
            return np.nan

    def _safe_get_stock_data(self, date, column_tuple):
        try:
            return self.stock_data.at[date, column_tuple]
        except (KeyError, IndexError):
            return np.nan

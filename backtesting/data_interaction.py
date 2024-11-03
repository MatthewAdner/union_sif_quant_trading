import pandas as pd
import numpy as np
from datetime import datetime

def str_to_date_obj(date_string:str):
    return(datetime.strptime(date_string, "%Y-%m-%d"))

def select_data_subset(input_dataframe, std_dev_day_range='all', reg_day_range='all', ticker_subset='all', price_vars_to_exclude=None, start_date=None, sort_cols = True):
    """
    Selects a subset of stock data based on a variety of factors.

    Args:
        input_dataframe (pandas dataframe, required): DataFrame with stock data.
        std_dev_day_range (str, list, optional): Leave as 'all' to include everything, give an int or a list of ints to select only std_devs over that/those periods. Defaults to 'all'.
        reg_day_range (list, optional): Leave as 'all' to include everything, give an int or a list of ints to select only reg values (intercept and coeffs) over that/those periods. Defaults to 'all'.
        ticker_subset (list, optional): Leave as 'all' to include all tickers, or give a list of ticker(s) to keep. Defaults to 'all'.
        price_vars_to_exclude (list, optional): Leave as None to include all price vars, or give a list of price variables to exclude. Defaults to None.
        start_date (str, optional): The date before which you don't want data, in the format 'YYYY-MM-DD'. Defaults to None.
        sort_cols (bool, optional): Set to true if you want to sort the columns alphabetically, first by Prive Var, then by ticker Defaults to True.
    Returns:
        pandas dataframe: Filtered DataFrame based on the specified criteria.
    """

    # filter date
    if start_date != None:
        return_df = input_dataframe[input_dataframe.index > start_date]
    else:
        return_df = input_dataframe
    
    return_cols = list(return_df.columns)

    # filter tickers
    if ticker_subset != 'all':
        # print(return_cols)
        return_cols = list(x for x in return_cols if x[1] in (ticker_subset+['']) )
    
    # exclude price vars
    if price_vars_to_exclude != None:
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
    
    if not((ticker_subset == 'all') and (price_vars_to_exclude == None) and (std_dev_day_range=='all') and (reg_day_range=='all')):
        return_df = return_df[return_cols]

    # sort columns if desired
    if sort_cols:
        return_df = return_df.sort_index(axis=1,level=[0,1])

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



class Company_Data_Getter:
    def __init__(self, company_data, stock_data):
        """helper object to get data on companies

        Args:
            company_data (dataframe): dataframe containing data on companies that doesn't change over time eg. sector
            stock_data (dataframe): 3d datafram containing data on companies that changes over time eg. market cap
        """
        self.company_data = company_data
        self.stock_data = stock_data
        
        self.large_countries = {
            "United States": "United States", "China": "China", "Canada": "Canada", "Israel": "Israel", "United Kingdom": "United Kingdom", "Singapore": "Singapore", "Hong Kong": "Hong Kong","Cayman Islands": "Cayman Islands", "Bermuda": "Bermuda",
            "Brazil": "Brazil","Australia": "Australia","Ireland": "Ireland","Netherlands": "Netherlands","Switzerland": "Switzerland","Greece": "Greece","Luxembourg": "Luxembourg","Japan": "Japan","Taiwan": "Taiwan","Germany": "Germany",
            "Malaysia": "Malaysia","Argentina": "Argentina","France": "France","Mexico": "Mexico"
        }
        
        # Grouped countries for regions
        self.regions = {
            "Caribbean and Latin America": [
                "Chile", "Colombia", "Uruguay", "Puerto Rico", "Panama", "Bahamas", "Costa Rica", "Curacao", "British Virgin Islands"
            ],
            "Western Europe": ["Belgium"],
            "Southern Europe": ["Italy", "Cyprus", "Monaco"],
            "Nordic Countries": ["Sweden", "Denmark", "Norway", "Finland"],
            "Channel Islands": ["Jersey", "Guernsey", "Isle of Man", "Gibraltar"],
            "East Asia": ["South Korea", "Macau"],
            "Southeast Asia": ["Thailand", "Philippines", "Indonesia"],
            "Oceania": ["New Zealand"],
            "Middle East": ["United Arab Emirates", "Turkey", "Jordan"],
            "Eastern Europe/Central Asia": ["Kazakhstan"],
            "Africa": ["South Africa"],
            "No Country Data":[""," "]
        }

        # Flatten the region dictionary to look up region by country
        self.country_to_region = {country: region for region, countries in self.regions.items() for country in countries}
        
    
        # Dictionary to store industries with less than 10 entries
        self.small_industries = {}

        # Get the value counts for industries in the company_data DataFrame
        industry_counts = self.company_data['Industry'].value_counts()

        # Loop through industries with fewer than 10 entries
        for industry, count in industry_counts.items():
            if count < 10:
                # Get the sector of the industry
                sector = self.company_data[self.company_data['Industry'] == industry]['Sector'].iloc[0]
                # Map industry to "Sector: Other"
                self.small_industries[industry] = f"{sector}: Other"
                
                

    # methods for series
    def get_name(self, tickers):
        return tickers.map(lambda ticker: self._safe_get_company_data(ticker, 'Name'))
        
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
    

    def get_country_region(self, tickers):
        """Returns a dictionary of regions for a list of tickers."""
        return {ticker: self.get_country_region_single(ticker) for ticker in tickers}
    
    def get_industry_category(self, tickers):
        """Returns a dictionary of categorized industries for a list of tickers."""
        return {ticker: self.get_industry_category_single(ticker) for ticker in tickers}


    # Methods for individual values
    def get_name_single(self, ticker):
        return self._safe_get_company_data(ticker, 'Name')
        
    def get_sector_single(self, ticker):
        return self._safe_get_company_data(ticker, 'Sector')

    def get_industry_single(self, ticker):
        return self._safe_get_company_data(ticker, 'Industry')

    def get_country_single(self, ticker):
        return self._safe_get_company_data(ticker, 'Country')

    def get_ipo_year_single(self, ticker):
        return self._safe_get_company_data(ticker, 'IPO_Year')

    def get_exchange_single(self, ticker):
        return self._safe_get_company_data(ticker, 'Exchange')

    def get_size_category_single(self, ticker, date):
        return self._safe_get_stock_data(date, ('Size_Category', ticker))

    def get_market_cap_single(self, ticker, date):
        return self._safe_get_stock_data(date, ('Market_Cap', ticker))

    def get_country_region_single(self, ticker):
        """Returns the region for a single ticker by first finding its country."""
        country = self.get_country_single(ticker)
        
        # Check if the country is in the large countries list
        if country in self.large_countries:
            return self.large_countries[country]
        
        # Check grouped regions for smaller countries
        return self.country_to_region.get(country, "Uncategorized Region")

    def get_industry_category_single(self, ticker):
        """Returns the categorized industry for a single ticker."""
        # Get the industry associated with the ticker
        industry = self.get_industry_single(ticker)
        
        # Check if the industry is categorized as a small industry
        if industry in self.small_industries:
            return self.small_industries[industry]
        
        # If not a small industry, return the industry as is
        return industry



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

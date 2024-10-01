# Import necessary libraries and functions

import pandas as pd
from statsmodels.regression.rolling import RollingOLS
from multiprocessing import Pool, cpu_count
from functools import partial
from tqdm import tqdm  # Import tqdm for the progress bar
from date_numbers import Date_Numbers  # Ensure this is picklable

# List of default values
default_reg_ranges = [5, 10, 30, 60, 90]
default_coefficient_list = ['Dates_Numeric']
default_to_predict = 'Adj_Close'

# Date converter object
date_numbers_obj = Date_Numbers()

def process_ticker(ticker, stock_data, reg_ranges, regression_string, coefficient_list, to_predict):
    regression_df = pd.DataFrame(index=stock_data.index)
    for reg_range in reg_ranges:
        stock_data_for_ticker = stock_data.xs(ticker, level=1, axis=1).join(stock_data.xs('', level=1, axis=1))
        if stock_data_for_ticker.empty or len(stock_data_for_ticker) < reg_range:
            continue
        try:
            ticker_model = RollingOLS.from_formula(regression_string, window=reg_range, data=stock_data_for_ticker)
            ticker_model_fit = ticker_model.fit()
            intercept_col_name = 'Intercept_' + str(reg_range) + '.' + ticker
            rename_dict = {'Intercept': intercept_col_name}
            for coef in coefficient_list:
                rename_dict[coef] = coef + '_Coeff_' + str(reg_range) + '.' + ticker
            params = ticker_model_fit.params.rename(columns=rename_dict)
            params['Std_Dev_' + str(reg_range) + '.' + ticker] = stock_data_for_ticker[to_predict].rolling(reg_range).std(ddof=0)
            params = params.shift()
            regression_df = pd.concat([regression_df, params], axis=1)
        except ValueError as e:
            print(f"Error processing {ticker}, window {reg_range}: {e}")
    return regression_df

def process_data(stock_data, output_file_name, reg_ranges=default_reg_ranges, coefficient_list=default_coefficient_list, to_predict=default_to_predict):
    stock_data.loc[:, 'Dates_Numeric'] = date_numbers_obj.date_to_num(stock_data.index.to_series())
    regression_string = to_predict + ' ~ ' + ' + '.join(coefficient_list)
    tickers = list({x[1] for x in stock_data.columns if x[1] != ''})
    with Pool(cpu_count()) as pool:
        partial_process_ticker = partial(
            process_ticker,
            stock_data=stock_data,
            reg_ranges=reg_ranges,
            regression_string=regression_string,
            coefficient_list=coefficient_list,
            to_predict=to_predict
        )
        results = list(tqdm(pool.imap(partial_process_ticker, tickers), total=len(tickers), desc="Processing Tickers"))
    regression_df = pd.concat(results, axis=1)
    rename_list = list((x.split('.')[0], x.split('.')[1]) for x in regression_df.columns if '.' in x)
    rename_commons_list = list((x, 'empty') for x in regression_df.columns if '.' not in x)
    rename_list += rename_commons_list
    regression_df.columns = pd.MultiIndex.from_tuples(rename_list)
    regression_df = regression_df.rename(columns={'empty': ''})
    stock_data = stock_data.join(regression_df)
    stock_data.to_pickle(output_file_name)

# Example call to the process_data function; replace stock_data and output_file_name as needed
# Ensure you call it here only when __name__ == "__main__"
# process_data(stock_data, 'stock_data_processed.pkl')

# Import libraries
import pandas as pd
from statsmodels.regression.rolling import RollingOLS
from multiprocessing import Pool, cpu_count
from functools import partial
from tqdm import tqdm  # Import tqdm for the progress bar

# Import class from other code
from date_numbers import Date_Numbers

# List of default values
default_reg_ranges = [5, 10, 30, 60, 90]
default_coefficient_list = ['Dates_Numeric']
default_to_predict = 'Adj_Close'

# Date converter object
date_numbers_obj = Date_Numbers()


def process_ticker(ticker, stock_data, reg_ranges, regression_string, coefficient_list, to_predict):
    regression_df = pd.DataFrame(index=stock_data.index)

    for reg_range in reg_ranges:
        # Takes slice of data for the ticker we're looking at and adds general info (e.g., weekday)
        stock_data_for_ticker = stock_data.xs(ticker, level=1, axis=1).join(stock_data.xs('', level=1, axis=1))
        
        # Check if data for ticker is empty or not sufficient for regression
        if stock_data_for_ticker.empty or len(stock_data_for_ticker) < reg_range:
            print(f"Skipping ticker {ticker} for window {reg_range}: insufficient data")
            continue
        
        # Debug: Print shape of the data
        print(f"Processing {ticker}, window {reg_range}, data shape: {stock_data_for_ticker.shape}")
        
        try:
            # Runs regression
            ticker_model = RollingOLS.from_formula(regression_string, window=reg_range, data=stock_data_for_ticker)
            ticker_model_fit = ticker_model.fit()

            # Sets up dictionary for renaming regression data (intercept and coefficients) column names so they are specific to this ticker
            intercept_col_name = 'Intercept_' + str(reg_range) + '.' + ticker
            rename_dict = {'Intercept': intercept_col_name}

            # Uses for loop in case we have many coefficients for many endogenous variables
            for coef in coefficient_list:
                rename_dict[coef] = coef + '_Coeff_' + str(reg_range) + '.' + ticker

            # Applies rename
            params = ticker_model_fit.params.rename(columns=rename_dict)

            # Calculates standard deviation
            params['Std_Dev_' + str(reg_range) + '.' + ticker] = stock_data_for_ticker[to_predict].rolling(reg_range).std(ddof=0)

            # Shifts the data and concatenates to regression_df
            params = params.shift()
            regression_df = pd.concat([regression_df, params], axis=1)
        except ValueError as e:
            print(f"Error processing {ticker}, window {reg_range}: {e}")

    return regression_df


def process_data(stock_data, output_file_name, reg_ranges=default_reg_ranges, coefficient_list=default_coefficient_list, to_predict=default_to_predict):
    stock_data.loc[:, 'Dates_Numeric'] = date_numbers_obj.date_to_num(stock_data.index.to_series())

    # Produces string of the format 'to_predict ~ coef_list[0] + coef_list[1]...'
    regression_string = to_predict + ' ~ ' + ' + '.join(coefficient_list)

    # List of tickers to process
    tickers = list({x[1] for x in stock_data.columns if x[1] != ''})

    # Use multiprocessing to process each ticker in parallel with a progress bar
    with Pool(cpu_count()) as pool:
        partial_process_ticker = partial(
            process_ticker,
            stock_data=stock_data,
            reg_ranges=reg_ranges,
            regression_string=regression_string,
            coefficient_list=coefficient_list,
            to_predict=to_predict
        )

        # Use tqdm to display progress while mapping the function
        results = list(tqdm(pool.imap(partial_process_ticker, tickers), total=len(tickers), desc="Processing Tickers"))

    # Concatenate all results into a single DataFrame
    regression_df = pd.concat(results, axis=1)

    # This renames columns and changes them to multi-index columns so they integrate with the rest of the data
    # Splits ticker-specific data into tuples
    rename_list = list((x.split('.')[0], x.split('.')[1]) for x in regression_df.columns if '.' in x)

    # Splits general data into tuples and sets the second value to 'empty'. Later we reset it to ''
    rename_commons_list = list((x, 'empty') for x in regression_df.columns if '.' not in x)
    rename_list += rename_commons_list

    # Puts tuples in as multi-index
    regression_df.columns = pd.MultiIndex.from_tuples(rename_list)

    # Resets 'empty' to ''
    regression_df = regression_df.rename(columns={'empty': ''})

    # Joins regression data with stock data
    stock_data = stock_data.join(regression_df)

    # Saves the processed stock data to a pickle file
    stock_data.to_pickle(output_file_name)


# Main entry point
if __name__ == "__main__":
    # Example call to the process_data function; replace stock_data and output_file_name as needed
    # process_data(stock_data, 'output_filename.pkl')
    pass  # Replace with actual function call when using this script

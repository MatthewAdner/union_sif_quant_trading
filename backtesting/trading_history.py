import pandas as pd
import numpy as np
from data_interaction import Company_Data_Getter


class Trading_History:
    def __init__(self, stock_data, company_data):
        """object to keep track of trades and perform trading analysis on them

        Args:
            stock_data (dataframe): stock data
            company_data (dataframe): data on companies
        """
        # Added 'Entry_Trading_Cost' and 'Exit_Trading_Cost' columns
        self.trades = pd.DataFrame(columns=['Ticker', 'Entry_Date', 'Entry_Share_Price', 'Entry_Trading_Cost',  'Shares', 'Exit_Date', 'Exit_Share_Price', 'Exit_Trading_Cost'])
        self.trades.index.name = 'Position_Name'
        self.company_data_getter_obj = Company_Data_Getter(stock_data=stock_data,company_data=company_data)
    
    def enter_position(self, date, ticker, shares, share_price, entry_trading_cost=0):
        # Create the index as a concatenation of ticker and date
        position_name = f"{ticker}_{date}"
        
        # Define the entry-related columns, including Entry_Trading_Cost
        entry_data = {
            'Ticker': ticker,
            'Entry_Date': date,
            'Entry_Share_Price': share_price,
            'Shares': shares,
            'Entry_Trading_Cost': entry_trading_cost
        }
        
        # Add the row to the DataFrame, leaving unspecified columns as NaN
        self.trades = pd.concat([self.trades, pd.DataFrame(entry_data, index=[position_name])])
    
    def exit_position(self, ticker, date_opened, date_closed, share_price, exit_trading_cost=0):
        # Create the index as a concatenation of ticker and date_opened to locate the entry
        position_name = f"{ticker}_{date_opened}"
        
        # Check if the position exists
        if position_name in self.trades.index:
            # Update the exit columns for the position, including Exit_Trading_Cost
            self.trades.at[position_name, 'Exit_Date'] = date_closed
            self.trades.at[position_name, 'Exit_Share_Price'] = share_price
            self.trades.at[position_name, 'Exit_Trading_Cost'] = exit_trading_cost
        else:
            print(f"Position {position_name} not found. Please check ticker and date_opened.")


    def add_analytics(self,to_include = ['Sector', 'Industry', 'Return', 'Annualized_Return','High_Share_Price', 'High_Share_Price_Date', 'Low_Share_Price','Low_Share_Price_Date', 'Volatility', 'Alpha', 'Beta', 'Sharpe', 'Sortino'], to_exclude=[]):
        # adds data like vol of positions during holding, high/low, high/low date, volatility, alpha, beta, sharpe, sortino
        # probably best to run this after 

        # add days held
        self.trades['Entry_Date'] = pd.to_datetime(self.trades['Entry_Date'])
        self.trades['Exit_Date'] = pd.to_datetime(self.trades['Exit_Date'])
        self.trades['Days_Held'] = (self.trades['Exit_Date'] - self.trades['Entry_Date']).dt.days

        # $ return
        self.trades['Return'] = (self.trades['Exit_Share_Price'] - self.trades['Entry_Share_Price'])*self.trades['Shares']
        # % return
        self.trades['Percent_Return'] = self.trades['Exit_Share_Price']/self.trades['Entry_Share_Price']-1
        # annualized % return
        self.trades['Annualized_Percent_Returns'] = (1 + self.trades['Percent_Return']) ** (365 / self.trades['Days_Held']) - 1

        # uses company_data_getter_obj to add company data
        self.trades['Sector'] = self.company_data_getter_obj.get_sector(self.trades['Ticker'])
        self.trades['Industry'] = self.company_data_getter_obj.get_industry(self.trades['Ticker'])
        self.trades['Country'] = self.company_data_getter_obj.get_country(self.trades['Ticker'])
        self.trades['Size_Category_Entry'] = self.company_data_getter_obj.get_size_category(self.trades['Ticker'],self.trades['Entry_Date'])


        

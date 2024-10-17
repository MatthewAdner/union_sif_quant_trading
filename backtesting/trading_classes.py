import pandas as pd
import datetime
import inspect
import trading_history


class Position:
    def __init__(self, date_opened, ticker, shares, stock_data, theo_var, std_dev_var, price_diff_var, company_data):
        """docstring for position

        Args:
            date_opened (_type_): _description_
            ticker (_type_): _description_
            shares (_type_): _description_
            stock_data (_type_): _description_
            theo_var (_type_): _description_
            std_dev_var (_type_): _description_
            price_diff_var (_type_): _description_
        """
        self.date_opened = date_opened
        self.ticker = ticker
        self.shares = shares
        self.stock_data = stock_data
        self.theo_var = theo_var
        self.std_dev_var = std_dev_var
        self.price_diff_var = price_diff_var
        self.cost_basis = shares*stock_data.at[date_opened,('Adj_Close',ticker)]
        self._current_value = shares*stock_data.at[date_opened,('Adj_Close',ticker)]
        self._current_theo = shares*stock_data.at[date_opened,(theo_var,ticker)]
        self._current_std_dev = shares*stock_data.at[date_opened,(std_dev_var,ticker)]
        self._current_price_diff = stock_data.at[date_opened,(price_diff_var,ticker)]
        self._last_date_checked = date_opened
        # TODO make methods- get_trading_cost that is a function of data from company_data
        self.company_data = company_data

    def __repr__(self):
        return (f"Position(date_opened={self.date_opened}, ticker='{self.ticker}', "
                f"shares={self.shares}, cost_basis={self.cost_basis}, "
                f"current_value={self._current_value}, "
                f"current_theo={self._current_theo}, "
                f"current_std_dev={self._current_std_dev}, "
                f"current_price_diff={self._current_price_diff}),"
                f"last_date_checked={self._last_date_checked}")

    def get_ticker(self):
        return(self.ticker)
    
    def days_old(self, date:str):
        """returns the number of days old a position is

        Args:
            date (str): date formated 'YYYY-MM-DD'
        """
        if type(self.date_opened) == str:
            opened = datetime.datetime.strptime(self.date_opened, '%Y-%m-%d').date()
        else:
            opened = self.date_opened
        if type(date)==str:
            current = datetime.datetime.strptime(date, '%Y-%m-%d').date()
        else:
            current = date
        return((current-opened).days)
    
    def __refresh__(self,current_date):
        self._last_date_checked = current_date
        self._current_value = self.shares*self.stock_data.at[current_date,('Adj_Close',self.ticker)]
        self._current_theo = self.stock_data.at[current_date,(self.theo_var, self.ticker)]
        self._current_std_dev = self.stock_data.at[current_date,(self.theo_var, self.ticker)]
        self._current_price_diff = self.stock_data.at[current_date,(self.price_diff_var, self.ticker)]

        # recalculate current_value as well as all other values that change over time,
        pass

    def get_current_value(self, current_date):
        self.__refresh__(current_date=current_date)
        return(self._current_value)
    
    def get_current_theo(self, current_date):
        self.__refresh__(current_date=current_date)
        return(self._current_theo)
    
    def get_current_std_dev(self, current_date):
        self.__refresh__(current_date=current_date)
        return(self._current_std_dev)

    def get_current_price_diff(self, current_date):
        self.__refresh__(current_date=current_date)
        return(self._current_price_diff)
    
    def get_trading_cost(self, current_date):
        # estimated trading costs
        trading_costs_dict = {
        'micro': 0.02,
        'small': 0.01,
        'mid': 0.005,
        'large': 0.002,
        'mega': 0.001
        }

        return(trading_costs_dict[
            self.stock_data.at[current_date,('Size_Category',self.ticker)]])





class Portfolio:
    def __init__(self, cash: float, date, stock_data, theo_var, std_dev_var, price_diff_var, company_data, trading_history_obj:trading_history.Trading_History = None):
        """create portfolio object

        Args:
            cash (float): starting amount of cash in account
            date (_type_): starting date
            # IGNOREtrading_cost (float, optional): cost of trading- each time we transact, we lose this amount. Defaults to 0.005.
        """
        self.position_df = pd.DataFrame(columns=['Position', 'Exposure', 'Value', 'Date_Opened', 'Days_Old'])
        self.position_df.index.name = 'Ticker'
        # Ensure 'Value' is explicitly set to float type
        self.position_df.loc['cash_position'] = ['N/A', 'N/A', float(cash), date, 0]  # Cast cash to float
        self.position_df = self.position_df.astype({'Value': 'float64'})  # Ensure Value column remains float
        self._last_date_checked = date
        # self.trading_cost = trading_cost
        self.stock_data = stock_data
        self.theo_var = theo_var
        self.std_dev_var = std_dev_var
        self.price_diff_var = price_diff_var
        self.company_data = company_data
        if (trading_history_obj!= None):
            self.trading_history_obj = trading_history_obj
            self.recording_trades = True
        else:
            self.recording_trades=False

    
    def __repr__(self):
        returnString = self.to_string(self._last_date_checked) + '\n' + str(self._last_date_checked)
        return returnString
        
    def get_cash(self):
        """returns size of cash_position
        """
        return(self.position_df.at['cash_position','Value'])

    def get_exposure(self):
        exposure = self.position_df['Value'].iloc[1:].sum()
        return(exposure)

    def position_count(self):
        return(len(self.position_df.index)-1)
    
    def open_position(self, date_opened, ticker, shares):
        """_summary_

        Args:
            date_opened (_type_): _description_
            ticker (_type_): _description_
            shares (_type_): _description_
        """ + inspect.getdoc(Position.__init__)
        position = Position(date_opened=date_opened, ticker=ticker, shares=shares, stock_data=self.stock_data, theo_var=self.theo_var, std_dev_var=self.std_dev_var, price_diff_var=self.price_diff_var,company_data=self.company_data)

        # Assuming position has attributes `cost_basis`, `get_ticker()`, and `date_opened`
        if position.cost_basis*(1+position.get_trading_cost(date_opened)) > self.position_df.loc['cash_position', 'Value']:
            print('Position not opened; too expensive')
            print(f"Available cash: {self.position_df.loc['cash_position', 'Value']}\nPosition cost: {position.cost_basis*(1+position.get_trading_cost(date_opened))} = position_cost ({position.cost_basis}) *(1+ trading_cost ({(position.get_trading_cost(date_opened))}))")
        else:
            # Explicit cast to ensure correct dtype when modifying Value
            self.position_df.loc['cash_position', 'Value'] = float(self.position_df.loc['cash_position', 'Value']) - (position.cost_basis * (1+position.get_trading_cost(date_opened)))
            # Add new row for the position
            self.position_df.loc[position.get_ticker()] = [position, position.shares, position.cost_basis, position.date_opened, 0]
            # log purchase
            if self.recording_trades:
                self.trading_history_obj.enter_position(date=date_opened.date(), ticker=ticker,shares=shares,share_price=position.cost_basis/position.shares,entry_trading_cost=position.get_trading_cost(date_opened))
        

    
    def close_position(self, ticker: str, current_date: str):
        """close a currently open position"""
        self._last_date_checked = current_date
        if ticker == 'cash_position':
            print('Cannot sell cash position')
        else:
            position_obj = self.position_df.loc[ticker, 'Position']
            position_value = position_obj.get_current_value(current_date)
            position_trading_cost = position_obj.get_trading_cost(current_date)
            self.position_df.loc['cash_position', 'Value'] += (position_value * (1-position_trading_cost))
            if self.recording_trades:
                self.trading_history_obj.exit_position(ticker=ticker,date_opened=position_obj.date_opened.date(), date_closed=current_date.date(), share_price=position_value/position_obj.shares, exit_trading_cost=position_trading_cost)

            self.position_df.drop(index=ticker, inplace=True)
    
    def get_portfolio_value(self, date):
        """gets the value of all the positions in the portfolio on the given date"""
        self.refresh_position_df(date)
        self._last_date_checked = date
        value = 0
        for ticker in self.position_df.index:
            value += self.position_df.loc[ticker, 'Value']
        return value
    
    def refresh_position_df(self, date):
        self._last_date_checked = date
        for ticker in self.position_df.index[1:]:  # Skipping the cash position
            self.position_df.loc[ticker, 'Value'] = self.position_df.loc[ticker, 'Position'].get_current_value(date)
            self.position_df.loc[ticker,'Days_Old'] = self.position_df.loc[ticker, 'Position'].days_old(date)

    def to_string(self, date: str):
        """returns a string summarizing portfolio's value"""
        self.refresh_position_df(date)
        return_string = f"Current Portfolio Value: {self.get_portfolio_value(date)}\n"
        return_string += f"Cash: {self.position_df.loc['cash_position', 'Value']}\n"
        for ticker in self.position_df.index[1:]:
            return_string += repr(self.position_df.loc[ticker, 'Position']) +'\n'
        return return_string

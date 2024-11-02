import pandas as pd
import datetime
import inspect
import trading_history

class Position:
    def __init__(self, date_opened, ticker, shares, stock_data, theo_var, std_dev_var, price_diff_var, company_data, stop_loss_threshold, take_profit_threshold, too_old=366):
        """docstring for position

        Args:
            date_opened (_type_): _description_
            ticker (_type_): _description_
            shares (_type_): _description_
            stock_data (_type_): _description_
            theo_var (_type_): _description_
            std_dev_var (_type_): _description_
            price_diff_var (_type_): _description_
            company_data (_type_): _description_
            stop_loss_threshold (float): (position's market value / position's cost basis) falls below this, we liquidate
            take_profit_threshold (float): (position's market value / position's cost basis) goes above this, we liquidate
            too_old (int): if a position has been held this many days and not been sold, it is sold. set to -1 if you don't want this feature

        """
        self.date_opened = date_opened
        self.ticker = ticker
        self.shares = shares
        self.stock_data = stock_data
        self.theo_var = theo_var
        self.std_dev_var = std_dev_var
        self.price_diff_var = price_diff_var
        self.cost_basis = shares*stock_data.at[date_opened,('Adj_Close',ticker)]
        self.stop_loss_threshold = stop_loss_threshold
        self.take_profit_threshold = take_profit_threshold
        self.too_old = too_old
        self._current_value = shares*stock_data.at[date_opened,('Adj_Close',ticker)]
        self._current_theo = shares*stock_data.at[date_opened,(theo_var,ticker)]
        self._current_std_dev = shares*stock_data.at[date_opened,(std_dev_var,ticker)]
        self._current_price_diff = stock_data.at[date_opened,(price_diff_var,ticker)]
        self._last_date_checked = date_opened
        # TODO make methods- get_trading_cost that is a function of data from company_data
        self.company_data = company_data
        self.position_name = ticker + '_' + (date_opened.strftime("%Y-%m-%d"))
        self.initial_theo = self.stock_data.at[date_opened,(self.theo_var,self.ticker)]
        self.high_water_mark = {'date':self.date_opened, 'share_price':self.cost_basis}
        self.low_water_mark = {'date':self.date_opened, 'share_price':self.cost_basis}


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
        self._current_share_price = self.stock_data.at[current_date,('Adj_Close',self.ticker)]
        self._current_theo = self.stock_data.at[current_date,(self.theo_var, self.ticker)]
        self._current_std_dev = self.stock_data.at[current_date,(self.theo_var, self.ticker)]
        self._current_price_diff = self.stock_data.at[current_date,(self.price_diff_var, self.ticker)]
        if self._current_share_price>self.high_water_mark['share_price']:
            self.high_water_mark['share_price'] = self._current_share_price
            self.high_water_mark['date'] = current_date
        if self._current_share_price<self.low_water_mark['share_price']:
            self.low_water_mark['share_price'] = self._current_share_price
            self.low_water_mark['date'] = current_date

        # recalculate current_value as well as all other values that change over time,
        pass

    def get_current_value(self, current_date):
        self.__refresh__(current_date=current_date)
        return(self._current_value)
    
    def get_current_share_price(self, current_date):
        self.__refresh__(current_date=current_date)
        return(self._current_share_price)
    
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
    
    #TODO: change this so it returns a tuple: either (False, None) or (True, 'Reason') where 'Reason' is 'old {details}', 'stop-loss {stop loss details}', 'take-profit {take profit details}'
    def is_it_time_to_sell(self,date):
        # too old
        if (self.days_old != -1) and (self.days_old(date)>=self.too_old):
            return(True)
        # stop-loss
        elif (self.get_current_value(date)/self.cost_basis)<=self.stop_loss_threshold:
            return(True)
        # take-profit
        elif self.take_profit_threshold=='initial_theo':
            # if value is above what our theoretical value was when we bought in
            return(self.get_current_value(date)>=self.initial_theo)
        
        elif (self.get_current_value(date)/self.cost_basis)>=self.take_profit_threshold:
            return(True)
        else:
            return(False)




class Portfolio:
    def __init__(self, cash: float, date, stock_data, theo_var, std_dev_var, price_diff_var, company_data, stop_loss_threshold=.5, take_profit_threshold=4, too_old = 366, trading_history_obj:trading_history.Trading_History = None, portfolio_name=None):
        """create portfolio object

        Args:
            cash (float): starting amount of cash in account
            date (_type_): starting date
            # IGNOREtrading_cost (float, optional): cost of trading- each time we transact, we lose this amount. Defaults to 0.005.
        """
        self.position_df = pd.DataFrame(columns=['Ticker', 'Position_Obj', 'Exposure', 'Value', 'Date_Opened', 'Days_Old'])
        self.starting_capital = cash
        # Ensure 'Value' is explicitly set to float type
        self.position_df.loc['cash_position'] = ['N/A', 'N/A', 'N/A', float(cash), date, 0]  # Cast cash to float
        self.position_df = self.position_df.astype({'Value': 'float64'})  # Ensure Value column remains float
        self._last_date_checked = date
        # self.trading_cost = trading_cost
        self.stock_data = stock_data
        self.theo_var = theo_var
        self.std_dev_var = std_dev_var
        self.price_diff_var = price_diff_var
        self.company_data = company_data
        self.portfolio_name = portfolio_name
        self.stop_loss_threshold = stop_loss_threshold
        self.take_profit_threshold = take_profit_threshold
        self.too_old_days = too_old

        ###########################################
        # self.historical_performance = pd.DataFrame(columns=[self.portfolio_name,'Date'])
        ########'''ADDED date as column for debugging purposes!!!!'''######
        self.historical_performance = pd.DataFrame(columns=[self.portfolio_name])


        self.historical_performance.index.name = 'Date'

        if (trading_history_obj!= None):
            self.trading_history_obj = trading_history_obj
            self.recording_trades = True
        else:
            self.recording_trades=False
        
        

    
    def __repr__(self):
        returnString = self.portfolio_name + ":\n" + self.to_string(self._last_date_checked) + '\n' + str(self._last_date_checked)
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
    
    def position_ticker_list(self):
        return(self.position_df['Ticker'].to_list()[1:]) ##??

    def get_position_name_list(self):
        return(self.position_df.index.to_list()[1:])
    
    def open_position(self, date_opened, ticker, shares, indicator=None):
        """_summary_

        Args:
            date_opened (_type_): _description_
            ticker (_type_): _description_
            shares (_type_): _description_
        """ + inspect.getdoc(Position.__init__)

        position = Position(date_opened=date_opened, ticker=ticker, shares=shares, stock_data=self.stock_data, theo_var=self.theo_var, std_dev_var=self.std_dev_var, price_diff_var=self.price_diff_var,company_data=self.company_data,
                            take_profit_threshold=self.take_profit_threshold,stop_loss_threshold=self.stop_loss_threshold,too_old=self.too_old_days)

        # Assuming position has attributes `cost_basis`, `get_ticker()`, and `date_opened`
        if shares==0:
            print(f"Can't buy 0 shares. Failed to buy {shares} of {ticker}")
        elif position.cost_basis*(1+position.get_trading_cost(date_opened)) > self.get_cash():
            print('Position not opened; too expensive')
            print(f"Available cash: {self.position_df.loc['cash_position', 'Value']}\nPosition cost: {position.cost_basis*(1+position.get_trading_cost(date_opened))} = position_cost ({position.cost_basis}) *(1+ trading_cost ({(position.get_trading_cost(date_opened))}))")
        else:
            # Explicit cast to ensure correct dtype when modifying Value
            self.position_df.loc['cash_position', 'Value'] = float(self.position_df.loc['cash_position', 'Value']) - (position.cost_basis * (1+position.get_trading_cost(date_opened)))
            # Add new row for the position
            # TODO
            # if we change the model such that it ever makes more than one trade in a day, this WILL cause non-obvious but serious problems. This sets the position info without checking if there already exists one. This isn't a problem now becuase there is never more than one set of trades made in a day, so there is never more than one trade on the same name in the same day.
            # if there were more than one trade in the same day (eg. buy 2 shares AAPL @ $100/share at the open, buy 3 shares AAPL @ $90/share at the close, we would pay $470 but the 3 shares would overwrite the 2 shares, so we'd only have 3 shares of apple)
            # if we do do this, then we need should keep track of trades in the same way as we are now, but with a timestamp
            # this change would also have to be made in the position class, at least where self.position_name is established, maybe elsewhere
            # print(str(date_opened.date()))
            self.position_df.loc[position.get_ticker() + '_' + str(date_opened.date())] = [ticker ,position, position.shares, position.cost_basis, position.date_opened, 0]##### ????? if date_opened is a date object and not a string this won't work, if that's the case, try adding/removing .date() ###???
            # log purchase
            if self.recording_trades:
                self.trading_history_obj.enter_position(date=date_opened.date(), ticker=ticker,shares=shares,share_price=position.cost_basis/position.shares,entry_trading_cost=position.get_trading_cost(date_opened), portfolio=self.portfolio_name, indicator=indicator)
        

    def positions_to_close(self, date):
        return_list = []
        for position_name in self.get_position_name_list():
            if self.position_df.at[position_name,'Position_Obj'].is_it_time_to_sell(date):
                return_list.append(position_name)
        return(return_list)

    
    def close_position(self, position_name: str, current_date: str):
        """close a currently open position

        Args:
            position_name (str): name of the position (the ticker and the date the position was opened in the format YYYY_MM_DD) in the format NVDA_1999_01_22
            current_date (str): the current date- the date you are selling it on in the format 'YYYY_MM_DD'
        """
        self._last_date_checked = current_date
        if position_name == 'cash_position':
            print('Cannot sell cash position')
        else:
            position_obj = self.position_df.loc[position_name, 'Position_Obj']
            ticker = position_obj.ticker
            position_value = position_obj.get_current_value(current_date)
            position_share_price = position_obj.get_current_share_price(current_date)
            position_trading_cost = position_obj.get_trading_cost(current_date)
            self.position_df.loc['cash_position', 'Value'] += (position_value * (1-position_trading_cost))
            if self.recording_trades:
                self.trading_history_obj.exit_position(ticker=ticker,date_opened=position_obj.date_opened.date(), date_closed=current_date.date(), share_price=position_share_price, exit_trading_cost=position_trading_cost,
                                                       high_water_mark=position_obj.high_water_mark,low_water_mark=position_obj.low_water_mark)

            self.position_df.drop(index=position_name, inplace=True)
    
    def close_positions(self, position_name_list: list, current_date: str):
        for position_name in position_name_list:
            self.close_position(position_name,current_date)

    def get_portfolio_value(self, date):
        """gets the value of all the positions in the portfolio on the given date"""
        self.refresh_position_df(date)
        self._last_date_checked = date
        value = 0
        for ticker in self.position_df.index:
            value += self.position_df.loc[ticker, 'Value']
        return value
    
    def add_value_snapshot(self, date):
        ##################################
        self.historical_performance.loc[date] = self.get_portfolio_value(date)
        # self.historical_performance.loc[self.historical_performance.shape[0]] = {
        #     self.portfolio_name:self.get_portfolio_value(date),
        #     'Date':date}
    
    
    def refresh_position_df(self, date):
        self._last_date_checked = date
        for ticker in self.position_df.index[1:]:  # Skipping the cash position
            self.position_df.loc[ticker, 'Value'] = self.position_df.loc[ticker, 'Position_Obj'].get_current_value(date)
            self.position_df.loc[ticker,'Days_Old'] = self.position_df.loc[ticker, 'Position_Obj'].days_old(date)

    def to_string(self, date: str):
        """returns a string summarizing portfolio's value"""
        self.refresh_position_df(date)
        return_string = f"Current Portfolio Value: {self.get_portfolio_value(date)}\n"
        return_string += f"Cash: {self.position_df.loc['cash_position', 'Value']}\n"
        for ticker in self.position_df.index[1:]:
            return_string += repr(self.position_df.loc[ticker, 'Position_Obj']) +'\n'
        return return_string



#TODO
'''
strategy class:
takes in stock_data and company_data, 

'''
class Strategy:
    def __init__(self):
        pass
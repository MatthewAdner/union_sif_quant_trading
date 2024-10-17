import pandas as pd
import quantstats as qs

def make_stats_dataframe(returns, benchmark_returns, portfolio_name="portfolio", decimals=3):
    """Outputs a DataFrame of statistics for a single portfolio.

    Args:
        returns (dataframe): portfolio; date as the index column, decimal change values from day to day as the value column.
        benchmark_returns (dataframe): benchmark; date as the index column, decimal change values from day to day as the value column.
        portfolio_name (str, optional): Name of the portfolio. Defaults to "portfolio".
        decimals (int, optional): Number of decimal places for the statistics. Defaults to 3.

    Returns:
        pd.DataFrame: DataFrame with a single row of statistics for the portfolio.
    """
    # Create a dictionary to store the statistics
    stats = {}

    # Calculate and store statistics
    stats["Annualized Return (CAGR)"] = round(qs.stats.cagr(returns) * 100, decimals)
    stats["Volatility (Standard Deviation)"] = round(qs.stats.volatility(returns) * 100, decimals)
    stats["Max Drawdown"] = round(qs.stats.max_drawdown(returns) * 100, decimals)
    
    # Alpha and Beta
    greeks = qs.stats.greeks(returns, benchmark_returns)
    stats["Alpha"] = round(greeks['alpha'], decimals)
    stats["Beta"] = round(greeks['beta'], decimals)
    
    # Sharpe and Sortino Ratios
    stats["Sharpe Ratio"] = round(qs.stats.sharpe(returns), decimals)
    stats["Sortino Ratio"] = round(qs.stats.sortino(returns), decimals)
    
    # Convert the stats dictionary to a DataFrame
    stats_df = pd.DataFrame(stats, index=[portfolio_name])
    
    return stats_df

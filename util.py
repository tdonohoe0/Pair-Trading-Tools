import os
import datetime
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression

stocks_df = pd.DataFrame()

def make_stock_df(folder_path, stock_codes):

    stocks_df = pd.DataFrame()

    for code in stock_codes:
        try:
            df = pd.read_csv(folder_path + code + ".us.txt")
            dates = pd.to_datetime(df.iloc[:, 0])

            df = df.drop(df.columns[0], axis=1)
            df_t = df.T

            df_t.columns = dates
            df_t.reset_index(drop=True, inplace=True)

            df_t.insert(0, 'Metric', df.columns)
            df_t.insert(0, 'Code', np.repeat(code, len(df.columns)))

            stocks_df = pd.concat([stocks_df, df_t], axis=0, ignore_index=True)
        except: # some stock files are devoid of data
            pass

    non_date_cols = ['Code', 'Metric']

    # Extract date columns
    date_cols = [col for col in stocks_df.columns if col not in non_date_cols]

    date_cols_sorted = sorted(date_cols, key=pd.to_datetime)
    return stocks_df[non_date_cols + date_cols_sorted]


### UTILITY FUNCS FOR OBTAINING LARGE DATABASE OF LAG DATA ###

# Assumes stock_df with columns "Code" to specify the company, "Metric" to specify the data metric, and then a series of dates
# to specify the value of the metric at that date
# stock_df actually needs to be a series

def log_dif(stock_df, n_diffs, earliest_date):

    # flip date order - need most current date at lowest index
    tseries_data = stock_df.iloc[2:]
    if tseries_data.index[0] < tseries_data.index[1]:
        tseries_data = tseries_data[::-1]
    
    #print("Well-ordered time series:")
    #print(tseries_data)

    # obtain first non-NaN value in the timeseries
    earliest_dt = pd.to_datetime(earliest_date)
    #first_valid_date = pd.Series([stock_df.iloc[2:].first_valid_index(), pd.to_datetime(earliest_date) + pd.offsets.BDay()]).max()
    first_valid_date = pd.Series([tseries_data.last_valid_index(), tseries_data.index[tseries_data.index > earliest_dt][-1]]).max()
    
    stock_slice = pd.to_numeric(tseries_data[:first_valid_date], errors='coerce')

    #print("sliced from earliest date time series:")
    #print(stock_slice)

    #print("\nstock_slice in log_dif:\n" + str(stock_slice))


    #logarithm transform
    stock_log_dif = np.log(stock_slice)


    ## I BELIEVE WE NEED TO DO EITHER diff(periods=-1) or just flip sign##
    for i in range(n_diffs):
        stock_log_dif = stock_log_dif.diff(periods=-1)
        #stock_log_dif = stock_log_dif.diff()
    
    #print("\nstock_log_dif: stock slice after transformation: \n" + str(stock_log_dif))

    #dif(log(timeseries)) with NaN values removed, early dates removed:
    return stock_log_dif[:first_valid_date].dropna()

#timeseries is a pure timeseries pandas series. Should include no metadata at the front end. 
def lag_df(timeseries, n):
    df = timeseries.to_frame()

    for i in range(n):
        df["lag" + str(i+1)] = timeseries.shift(-(i+1))

    df.rename(columns={df.columns[0]: "val"}, inplace=True)
    
    return df.iloc[:-n]


# combines the above to obtain lags df on a certain metric for full df
# consider adding a n_diffs argument. I will not do for now
def stocks_df_to_lags(stocks_df, metric, earliest_date, n_lags):

    rv = pd.DataFrame()

    def accumulator_func(row):
        nonlocal rv
        new_lag_df = lag_df(log_dif(row, 1, earliest_date), n_lags)
        new_lag_df["Code"] = row["Code"]
        rv = pd.concat([rv, new_lag_df])
        #print(row)
        #print(new_lag_df)

    stocks_df[stocks_df["Metric"] == metric].apply(accumulator_func, axis=1)
    rv.reset_index()

    if 'index' in rv.columns:
        # Rename the "index" column to something else, e.g., "new_index"
        rv.rename(columns={'index': 'Date'}, inplace=True)

    return rv.reset_index()



### UTILITY FUNCS FOR OBTAINING QUARTERLY DATA FROM DAILY DATA ###

# expects stock_df of our standard form: dated columns, rows for each company and metric
# end-of-quarter prices: first day of april, july, october, and jan
def day_df_to_quarters(df, fixed_day_in_quarter=0):
    date_cols = df.columns[2:].to_series()
    date_by_q = date_cols.groupby(date_cols.dt.to_period('Q')).nth(fixed_day_in_quarter)
    print(date_by_q)

    rename_dict = {v:k for k, v in date_by_q.items()}
    
    #print(df.iloc[:, :2])
    #print(df.loc[: , first_date_by_q].rename(columns=rename_dict))
    return pd.concat([df.iloc[:, :2], df.loc[: , date_by_q]], axis=1)

def day_df_to_years(df, fixed_day_in_year=0):
    date_cols = df.columns[2:].to_series()
    date_by_y = date_cols.groupby(date_cols.dt.to_period('Y')).nth(fixed_day_in_year)

    rename_dict = {v:k for k, v in date_by_y.items()}
    
    #print(df.iloc[:, :2])
    #print(df.loc[: , first_date_by_q].rename(columns=rename_dict))
    return pd.concat([df.iloc[:, :2], df.loc[: , date_by_y]], axis=1)


def logdif_to_pct_growth(val, short=False):
    if short == False:
        return np.exp(val) - 1
    else:
        return 1 - np.exp(val)
    

### For pair trading primarily
def last_n_regression(tseriesx, tseriesy, index, n):
    model = LinearRegression()
    model.fit(tseriesx.loc[:index].iloc[-n:].to_frame(), tseriesy.loc[:index].iloc[-n:])
    return model.intercept_, model.coef_[0]

def last_n_std(tseries, index, n):
    return np.std(tseries.loc[:index].iloc[-n:])

def last_n_mean(tseries, index, n):
    return np.mean(tseries.loc[:index].iloc[-n:])


### get close timeseries
def get_close_timeseries(data, include_code=False):
    # Extract the stock symbol
    symbol = data["Meta Data"]["2. Symbol"]
    
    time_series = None
    # Extract the time series data
    if "Time Series (1min)" in data.keys():
        time_series = data["Time Series (1min)"]
    if "Time Series (Daily)" in data.keys():
        time_series = data["Time Series (Daily)"]
    
    # Create a dictionary to hold datetime and close values
    close_data = {}
    
    # Add the stock symbol at the beginning of the series
    if include_code == True:
        close_data["Code"] = symbol
    
    for time_str, values in time_series.items():
        # Convert the time string to a datetime object
        time_dt = pd.to_datetime(time_str)
        
        # Extract the close value
        close_value = float(values["4. close"])
        
        # Store in dictionary
        close_data[time_dt] = close_value
    
    # Convert the dictionary to a pandas Series
    close_series = pd.Series(close_data)
    
    return close_series

import pandas as pd
import requests
import time

### Highest-level function... only function that should need to be called from pair_trading_pipeline ###
### Or pair_trading_tester ###
def get_timeseries(api_key, api_name, ticker=None, params=None):
    if api_name == "TIME_SERIES_DAILY":
        return daily(api_key, ticker, params)
        

### each of these functions maps to an AlphaVantage API... TODO - add more than just TIME_SERIES_DAILY ###

def daily(api_key, ticker, params):
    default_params = "&outputsize=full&apikey=" + api_key
    if not params is None:
        default_params = params + "&apikey=" + api_key
    url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=" + ticker + default_params
    return get_close_timeseries(requests.get(url).json()).sort_index()


### getting pandas series from api json ###
# TODO - this could be made flexible to get more than just close timeseries
def get_close_timeseries(data):
    time_series = None
    # Extract the time series data. can handle several APIs' formats
    if "Time Series (1min)" in data.keys():
        time_series = data["Time Series (1min)"]
    if "Time Series (Daily)" in data.keys():
        time_series = data["Time Series (Daily)"]
    
    # Create a dictionary to hold datetime and close values
    close_data = {}
    
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


### Random utility function ###
def obtain_split_dates(api_key, codes):
    code_to_split_dates = {}
    def accumulator_func(code):
        json = requests.get('https://www.alphavantage.co/query?function=SPLITS&symbol=' + code + '&apikey=' + api_key).json()
        while 'Information' in json.keys() and json['Information'] == 'Thank you for using Alpha Vantage! Please contact premium@alphavantage.co if you are targeting a higher API call volume.':
            time.sleep(5)
            json = requests.get('https://www.alphavantage.co/query?function=SPLITS&symbol=' + code + '&apikey=' + api_key).json()
        data = json["data"]
        code_to_split_dates[code] = {pd.to_datetime(split['effective_date']):float(split['split_factor']) for split in data}

    codes.apply(accumulator_func)
    return code_to_split_dates
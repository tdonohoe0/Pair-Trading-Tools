import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
import util

class PairStrategy:
    def __init__(self, tickers, methodology, args):
        self.tickers = tickers #2 element list
        self.methodology = methodology #function which takes 2 timeseries, 2 positions, and other args. Computes next position
        self.args = args #other arguments to methodology

    def compute_positions(self, timeseries1, timeseries2, position1, position2):
        return self.methodology(timeseries1, timeseries2, position1, position2, **self.args)

def bollinger_positions(zscores, coefs, entry, exit):
    position = 0  # 0 means no position, 1 means long-short entered
    positions1 = []
    positions2 = []

    for z, coef in zip(zscores, coefs):
        if np.abs(z) > entry and not z/np.abs(z) == position:
            positions1.append( - (coef * z)/np.abs(z))
            positions2.append(z/np.abs(z))
            position = z/np.abs(z)
        elif position != 0 and np.abs(z) < exit:
            positions1.append(positions1[-1])
            positions2.append(positions2[-1])
            position = 0
        elif len(positions1) == 0 or position == 0:
            positions1.append(0)
            positions2.append(0)
        else:
            positions1.append(positions1[-1])
            positions2.append(positions2[-1])

    return pd.Series(positions1, index=zscores.index), pd.Series(positions2, index=zscores.index)

def bollinger_linear(timeseries1, timeseries2, position1, position2, lookback, entry, exit):

    # Make sure that timeseries face right way - low numbers first
    timeseries1.sort_index()
    timeseries2.sort_index()

    # Only calculates position of next day
    lookback1 = timeseries1.iloc[-lookback:]
    #print("lookback1" + str(lookback1))
    lookback2 = timeseries2.iloc[-lookback:]
    #print("lookback2" + str(lookback2))

    intercept, coef = util.last_n_regression(timeseries1, timeseries2, timeseries1.index[-1], lookback)
    spreads = lookback1*coef - lookback2 + intercept

    std = util.last_n_std(spreads, timeseries1.index[-1], lookback)
    mean = util.last_n_mean(spreads, timeseries1.index[-1], lookback)
    #print("mean: " + str(mean))
    #print("spread: " + str(spreads.iloc[-1]))
    #print("std: " + str(std))
    z = (spreads.iloc[-1] - mean)/std
    #print("z: " + str(z))

    ## overall position - if positive, then short stock 1 and long stock 2
    position = 0 if position2 == 0 else position2/np.abs(position2)

    ### BUY ###
    if np.abs(z) > entry and not z*position > 0:
        return ( - (coef * z)/np.abs(z), z/np.abs(z))
    ### SELL ###
    elif position != 0 and position*z < exit:
      return (0,0)
    else:
        return (position1, position2)

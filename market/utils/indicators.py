# market/utils/indicators.py
import pandas as pd
import numpy as np

def calculate_rsi(closes, period=14):
    """Calculate RSI(14) from list of close prices"""
    if len(closes) < period + 1:
        return None
    deltas = np.diff(closes)
    seed = deltas[:period+1]
    up = seed[seed >= 0].sum() / period
    down = -seed[seed < 0].sum() / period
    rs = up / down if down != 0 else np.inf
    rsi = np.zeros_like(closes)
    rsi[:period] = 100. - 100. / (1. + rs)
    
    for i in range(period, len(closes)):
        delta = deltas[i - 1]
        if delta > 0:
            upval = delta
            downval = 0.
        else:
            upval = 0.
            downval = -delta
        up = (up * (period - 1) + upval) / period
        down = (down * (period - 1) + downval) / period
        rs = up / down if down != 0 else np.inf
        rsi[i] = 100. - 100. / (1. + rs)
    return rsi[-1] if len(rsi) > 0 else None

def calculate_macd(closes, fast=12, slow=26, signal=9):
    """Calculate MACD line, signal line, histogram"""
    if len(closes) < slow:
        return None, None, None
    ema_fast = pd.Series(closes).ewm(span=fast, adjust=False).mean()
    ema_slow = pd.Series(closes).ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line
    return macd_line.iloc[-1], signal_line.iloc[-1], histogram.iloc[-1]

def calculate_adx(highs, lows, closes, period=14):
    """Calculate ADX, +DI, -DI"""
    if len(closes) < period * 2:
        return None, None, None
    
    df = pd.DataFrame({'high': highs, 'low': lows, 'close': closes})
    
    # True Range
    df['tr1'] = df['high'] - df['low']
    df['tr2'] = abs(df['high'] - df['close'].shift())
    df['tr3'] = abs(df['low'] - df['close'].shift())
    df['tr'] = df[['tr1', 'tr2', 'tr3']].max(axis=1)
    
    # Directional Movement
    df['dm_plus'] = np.where((df['high'] - df['high'].shift()) > (df['low'].shift() - df['low']),
                             np.maximum(df['high'] - df['high'].shift(), 0), 0)
    df['dm_minus'] = np.where((df['low'].shift() - df['low']) > (df['high'] - df['high'].shift()),
                              np.maximum(df['low'].shift() - df['low'], 0), 0)
    
    # Smoothed values
    atr = df['tr'].rolling(window=period).mean()
    di_plus = 100 * (df['dm_plus'].rolling(window=period).mean() / atr)
    di_minus = 100 * (df['dm_minus'].rolling(window=period).mean() / atr)
    dx = 100 * abs(di_plus - di_minus) / (di_plus + di_minus)
    adx = dx.rolling(window=period).mean()
    
    return adx.iloc[-1], di_plus.iloc[-1], di_minus.iloc[-1]
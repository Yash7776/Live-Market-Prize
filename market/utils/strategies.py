# market/utils/strategies.py

def check_adx_strategy(adx_info, current_side='NONE'):
    """
    ADX Strategy:
    - No position → BUY if DI+ > 20, SELL if DI- > 20
    - LONG position → EXIT if DI+ < 18
    - SHORT position → EXIT if DI- < 18
    """
    adx = adx_info.get('adx')
    di_plus = adx_info.get('di_plus')
    di_minus = adx_info.get('di_minus')

    if any(x is None for x in [adx, di_plus, di_minus]):
        return None  # not enough data

    signal = None

    if current_side == 'NONE':
        if di_plus > 20:
            signal = {
                "action": "BUY",
                "reason": f"+DI {di_plus:.2f} > 20 (strong uptrend)",
                "confidence": "high" if di_plus > di_minus else "medium"
            }
        elif di_minus > 20:
            signal = {
                "action": "SELL",
                "reason": f"-DI {di_minus:.2f} > 20 (strong downtrend)",
                "confidence": "high" if di_minus > di_plus else "medium"
            }

    elif current_side == "LONG":
        if di_plus < 18:
            signal = {
                "action": "EXIT",
                "reason": f"+DI fell to {di_plus:.2f} < 18 (uptrend weakening)"
            }

    elif current_side == "SHORT":
        if di_minus < 18:
            signal = {
                "action": "EXIT",
                "reason": f"-DI fell to {di_minus:.2f} < 18 (downtrend weakening)"
            }

    return signal

def check_macd_strategy(macd_info, current_side='NONE'):
    """
    MACD Strategy:
    - No position → BUY if MACD line > 0, SELL if MACD line < 0
    - LONG → EXIT if MACD line < 0
    - SHORT → EXIT if MACD line > 0
    """
    macd_line = macd_info.get('line')
    if macd_line is None:
        return None

    signal = None

    if current_side == 'NONE':
        if macd_line > 0:
            signal = {"action": "BUY", "reason": f"MACD line {macd_line:.4f} > 0 (bullish)"}
        elif macd_line < 0:
            signal = {"action": "SELL", "reason": f"MACD line {macd_line:.4f} < 0 (bearish)"}

    elif current_side == "LONG":
        if macd_line < 0:
            signal = {"action": "EXIT", "reason": "MACD line crossed below 0 (bearish crossover)"}

    elif current_side == "SHORT":
        if macd_line > 0:
            signal = {"action": "EXIT", "reason": "MACD line crossed above 0 (bullish crossover)"}

    return signal
# market/utils/strategies.py
from logzero import logger

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

    # Check if any critical values are None or string (not enough data)
    if any(isinstance(x, str) or x is None for x in [di_plus, di_minus]):
        return None  # not enough data

    signal = None

    if current_side == 'NONE':
        if di_plus > 20 and di_plus > di_minus:
            signal = {
                "action": "BUY",
                "reason": f"+DI {di_plus:.2f} > 20 (strong uptrend)",
                "confidence": "high"
            }
        elif di_minus > 20 and di_minus > di_plus:
            signal = {
                "action": "SELL",
                "reason": f"-DI {di_minus:.2f} > 20 (strong downtrend)",
                "confidence": "high"
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
    logger.info(f"ADX Signal: {signal}")
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
        logger.warning("MACD line is None - not enough data")
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
    logger.info(f"MACD Signal: {signal}")
    return signal
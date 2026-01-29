import json
import os
import threading
import pyotp
import requests
from dotenv import load_dotenv
import pandas as pd
import numpy as np
from channels.generic.websocket import WebsocketConsumer
from logzero import logger
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# Load environment variables from .env file
load_dotenv()

# ────────────────────────────────────────────────
# Your provided credentials (store in .env for security!)
# ────────────────────────────────────────────────
API_KEY = os.getenv("ANGEL_API_KEY")                    
CLIENT_CODE = os.getenv("ANGEL_CLIENT_CODE")            
PIN = os.getenv("ANGEL_PIN")                            
TOTP_SECRET = os.getenv("ANGEL_TOTP")                   

INSTRUMENT_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"

# Official exchangeType mapping from SmartAPI SDK source
EXCHANGE_MAP = {
    "NSE": 1,    # NSE_CM - Cash / Equity + Indices
    "NFO": 2,    # NSE_FO - Futures & Options
    "BSE": 3,    # BSE_CM - Cash / Equity
    "BFO": 4,    # BSE_FO - Futures & Options
    "MCX": 5,    # MCX_FO - Commodity Futures (also used for energy)
    "NCO": 5,    # NCO (energy/commodity segment) - same type as MCX
    "NCDEX": 7,  # NCX_FO - NCDEX Futures
    "CDS": 13,   # CDE_FO - Currency Derivatives
    # Add more only if needed and confirmed
}


class MarketConsumer(WebsocketConsumer):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.positions = {}  # Initialize position tracker here
    
    def connect(self):
        self.accept()
        self.send(json.dumps({"status": "WebSocket Connected - Starting login..."}))

        self.subscribed_tokens = {}          # exchangeType: set(tokens)
        self.token_symbol_map = {}           # token: symbol (tradingsymbol)
        self.smart_api = None
        self.sws = None
        self.instrument_list = None
        self.auth_token = None
        self.feed_token = None

        # Start SmartAPI login + socket in background thread
        threading.Thread(target=self.start_smartapi, daemon=True).start()

    def start_smartapi(self):
        try:
            # ────────────────────────────────────────────────
            # STEP 2: Generate Access Token (Login)
            # ────────────────────────────────────────────────
            self.smart_api = SmartConnect(api_key=API_KEY)

            totp_code = pyotp.TOTP(TOTP_SECRET).now()
            logger.info(f"Generated TOTP: {totp_code}")

            login_data = self.smart_api.generateSession(CLIENT_CODE, PIN, totp_code)

            if login_data.get('status') == False:
                error_msg = login_data.get('message', 'Login failed - check credentials/TOTP')
                logger.error(error_msg)
                self.send(json.dumps({"error": error_msg, "login_status": "FAILED"}))
                return

            self.auth_token = login_data["data"]["jwtToken"]
            self.feed_token = self.smart_api.getfeedToken()

            logger.info(f"Login SUCCESS - Auth Token: {self.auth_token[:20]}... | Feed Token: {self.feed_token[:20]}...")
            self.send(json.dumps({
                "status": "Login Successful",
                "login_status": "SUCCESS",
                "client_code": CLIENT_CODE
            }))

            # Fetch instrument master list (for token lookup)
            response = requests.get(INSTRUMENT_URL)
            if response.status_code == 200:
                self.instrument_list = response.json()
                logger.info(f"Instrument list fetched - {len(self.instrument_list)} entries")
            else:
                logger.error("Failed to fetch instrument list")
                self.send(json.dumps({"error": "Instrument list download failed"}))

            # ────────────────────────────────────────────────
            # STEP 3: Start WebSocket Datafeed
            # ────────────────────────────────────────────────
            self.sws = SmartWebSocketV2(
                self.auth_token,
                API_KEY,
                CLIENT_CODE,
                self.feed_token
            )

            correlation_id = "market_stream_001"
            mode = 1  # LTP mode (change to 2=Depth, 3=OI if needed later)

            def on_open(wsapp):
                logger.info("SmartAPI WebSocket Opened - Ready for subscriptions")
                self.send(json.dumps({"status": "Datafeed Connected"}))

            def on_data(wsapp, message):
                token = message.get("token")
                ltp = message.get("last_traded_price")

                if not ltp:
                    return

                price = ltp / 100

                symbol = self.token_symbol_map.get(token, "UNKNOWN")

                self.send(text_data=json.dumps({
                    "symbol": symbol,
                    "token": token,
                    "ltp": price
                }))


            def on_error(wsapp, error):
                logger.error(f"WebSocket Error: {error}")
                self.send(text_data=json.dumps({"error": str(error)}))

            def on_close(wsapp):
                logger.info("SmartAPI WebSocket Closed - Attempting reconnect in 5 seconds...")
                self.send(text_data=json.dumps({"status": "Datafeed Disconnected - Reconnecting..."}))

                # Simple reconnection (can be made more robust)
                import time
                time.sleep(5)
                try:
                    logger.info("Reconnecting WebSocket...")
                    self.sws.connect()
                except Exception as e:
                    logger.error(f"Reconnect failed: {e}")
                    self.send(text_data=json.dumps({"error": f"Reconnect failed: {str(e)}"}))

            # Also add this to make reconnection more reliable
            self.sws.on_close = on_close

            self.sws.on_open = on_open
            self.sws.on_data = on_data
            self.sws.on_error = on_error
            self.sws.on_close = on_close

            self.sws.connect()

        except Exception as e:
            logger.error(f"Critical error in start_smartapi: {str(e)}")
            self.send(text_data=json.dumps({
                "error": f"Login/Datafeed failed: {str(e)}",
                "login_status": "FAILED"
            }))

    def receive(self, text_data):
        try:
            data = json.loads(text_data)
            action = data.get("action")

            if action == "subscribe":
                tradingsymbols = data.get("tradingsymbols", [])
                exchange = data.get("exchange", "NSE")
                self.handle_subscribe(tradingsymbols, exchange)

            elif action == "unsubscribe":
                tradingsymbols = data.get("tradingsymbols", [])
                exchange = data.get("exchange", "NSE")
                self.handle_unsubscribe(tradingsymbols, exchange)

            elif action == "place_order":
                params = data.get("params", {})
                self.handle_place_order(params)

            elif action == "modify_order":
                params = data.get("params", {})
                self.handle_modify_order(params)

            elif action == "cancel_order":
                params = data.get("params", {})
                self.handle_cancel_order(params)

            elif action == "get_order":
                params = data.get("params", {})
                self.handle_get_order_details(params)
                
            elif action == "relogin":
                self.handle_relogin()

            elif action == "get_historical":
                params = data.get("params", {})
                self.handle_get_historical(params)

            else:
                self.send(text_data=json.dumps({"error": f"Unknown action: {action}"}))

        except Exception as e:
            self.send(text_data=json.dumps({
                "error": str(e)
            }))

    def handle_subscribe(self, tradingsymbols, exchange):
        if not self.instrument_list:
            return self.send(text_data=json.dumps({"error": "Instrument list not loaded"}))

        exchange_type = EXCHANGE_MAP.get(exchange, 1)
        tokens = []
        new_token_symbol_map = {}

        for ts in tradingsymbols:
            for instr in self.instrument_list:
                if instr["symbol"] == ts and instr["exch_seg"] == exchange:
                    token = instr["token"]
                    if exchange_type not in self.subscribed_tokens or token not in self.subscribed_tokens[exchange_type]:
                        tokens.append(token)
                    new_token_symbol_map[token] = ts
                    break

        if tokens:
            token_list = [{"exchangeType": exchange_type, "tokens": tokens}]
            self.sws.subscribe("abc123", 1, token_list)  # mode=1 for LTP

            if exchange_type not in self.subscribed_tokens:
                self.subscribed_tokens[exchange_type] = set()
            self.subscribed_tokens[exchange_type].update(tokens)

            self.token_symbol_map.update(new_token_symbol_map)

            self.send(text_data=json.dumps({"status": "subscribed", "tradingsymbols": tradingsymbols}))

    def handle_unsubscribe(self, tradingsymbols, exchange):
        if not self.instrument_list:
            return self.send(text_data=json.dumps({"error": "Instrument list not loaded"}))

        exchange_type = EXCHANGE_MAP.get(exchange, 1)
        tokens = []

        for ts in tradingsymbols:
            for instr in self.instrument_list:
                if instr["symbol"] == ts and instr["exch_seg"] == exchange:
                    token = instr["token"]
                    if exchange_type in self.subscribed_tokens and token in self.subscribed_tokens[exchange_type]:
                        tokens.append(token)
                    break

        if tokens:
            token_list = [{"exchangeType": exchange_type, "tokens": tokens}]
            self.sws.unsubscribe("abc123", 1, token_list)  # mode=1 for LTP

            self.subscribed_tokens[exchange_type].difference_update(tokens)
            if not self.subscribed_tokens[exchange_type]:
                del self.subscribed_tokens[exchange_type]

            for token in tokens:
                if token in self.token_symbol_map:
                    del self.token_symbol_map[token]

            self.send(text_data=json.dumps({"status": "unsubscribed", "tradingsymbols": tradingsymbols}))

    def handle_place_order(self, params):
        try:
            order_response = self.smart_api.placeOrder(params)
            logger.info(f"Raw broker response from placeOrder: {order_response}")

            if order_response is None:
                error_msg = "placeOrder returned None - possible session invalid, network issue, or broker restriction"
                self.send(text_data=json.dumps({
                    "error": error_msg,
                    "broker_response": None
                }))
                logger.error(error_msg)
                return

            # Now safely check if it's a dict
            if isinstance(order_response, dict):
                if order_response.get('status') is True:
                    order_id = order_response.get('data', {}).get('orderid')
                    if order_id:
                        self.send(text_data=json.dumps({
                            "status": "order_placed",
                            "orderid": order_id,
                            "data": order_response
                        }))
                        logger.info(f"Order placed successfully - ID: {order_id}")
                    else:
                        self.send(text_data=json.dumps({
                            "error": "Order accepted but no orderid returned",
                            "broker_response": order_response
                        }))
                else:
                    error_msg = order_response.get('message', 'Order placement rejected by broker')
                    self.send(text_data=json.dumps({
                        "error": error_msg,
                        "errorcode": order_response.get('errorcode'),
                        "broker_response": order_response
                    }))
                    logger.error(f"Order rejected: {order_response}")
            else:
                self.send(text_data=json.dumps({
                    "error": f"Unexpected response type from placeOrder: {type(order_response)}",
                    "broker_response": str(order_response)
                }))
                logger.error(f"Unexpected type: {type(order_response)} - {order_response}")

        except Exception as e:
            self.send(text_data=json.dumps({
                "error": f"Exception during placeOrder: {str(e)}",
                "action": "place_order"
            }))
            logger.error(f"Place order exception: {str(e)}")

    def handle_modify_order(self, params):
        try:
            order_id = params.get("orderid")
            if not order_id:
                raise ValueError("orderid is required for modify")

            # Remove orderid from params before passing to modifyOrder
            modify_params = {k: v for k, v in params.items() if k != "orderid"}

            response = self.smart_api.modifyOrder(order_id, modify_params)
            self.send(text_data=json.dumps({
                "status": "order_modified",
                "orderid": order_id,
                "data": response
            }))
            logger.info(f"Modify Order Success: {order_id} - {response}")
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Modify Order Failed: {error_msg}")
            self.send(text_data=json.dumps({
                "error": error_msg,
                "action": "modify_order"
            }))

    def handle_cancel_order(self, params):
        try:
            order_id = params.get("orderid")
            variety = params.get("variety", "NORMAL")  # default to NORMAL

            if not order_id:
                raise ValueError("orderid is required for cancel")

            response = self.smart_api.cancelOrder(variety, order_id)
            self.send(text_data=json.dumps({
                "status": "order_canceled",
                "orderid": order_id,
                "data": response
            }))
            logger.info(f"Cancel Order Success: {order_id} - {response}")
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Cancel Order Failed: {error_msg}")
            self.send(text_data=json.dumps({
                "error": error_msg,
                "action": "cancel_order"
            }))

    def handle_get_order_details(self, params):
        try:
            order_id = params.get("orderid")
            if not order_id:
                raise ValueError("orderid is required for details")

            # Get full order book and filter for the orderid
            order_book = self.smart_api.orderBook()
            if not order_book or 'data' not in order_book:
                raise ValueError("No order book data")

            matching_order = None
            for order in order_book['data']:
                if order.get('orderid') == order_id:
                    matching_order = order
                    break

            if matching_order:
                self.send(text_data=json.dumps({
                    "status": "order_details",
                    "orderid": order_id,
                    "data": matching_order
                }))
            else:
                self.send(text_data=json.dumps({
                    "error": f"Order {order_id} not found in recent orders",
                    "action": "get_order"
                }))
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Get Order Details Failed: {error_msg}")
            self.send(text_data=json.dumps({
                "error": error_msg,
                "action": "get_order"
            }))
    def handle_relogin(self):
        try:
            totp_code = pyotp.TOTP(TOTP_SECRET).now()
            logger.info(f"Re-login TOTP: {totp_code}")
            login_data = self.smart_api.generateSession(CLIENT_CODE, PIN, totp_code)

            if login_data.get('status') == True:
                self.auth_token = login_data["data"]["jwtToken"]
                self.feed_token = self.smart_api.getfeedToken()
                self.send(text_data=json.dumps({
                    "status": "Re-login successful - session refreshed",
                    "auth_token_prefix": self.auth_token[:20] + "..."
                }))
                logger.info("Re-login SUCCESS")
            else:
                error_msg = login_data.get('message', 'Re-login failed - check credentials/TOTP')
                self.send(text_data=json.dumps({"error": error_msg}))
                logger.error(error_msg)

        except Exception as e:
            self.send(text_data=json.dumps({"error": f"Re-login exception: {str(e)}"}))
            logger.error(f"Re-login exception: {str(e)}")
            
    def handle_get_historical(self, params):
        try:
            # Required params from frontend
            symbol_token = params.get("symboltoken")
            exchange = params.get("exchange", "NSE")
            interval = params.get("interval", "FIFTEEN_MINUTE")
            from_date = params.get("from_date")   # e.g. "2026-01-01 09:00"
            to_date   = params.get("to_date")     # e.g. "2026-01-28 15:30"

            if not all([symbol_token, from_date, to_date]):
                raise ValueError("symboltoken, from_date, to_date are required")

            # Correct format: single dictionary for getCandleData
            historic_params = {
                "exchange": exchange,
                "symboltoken": symbol_token,
                "interval": interval,
                "fromdate": from_date,
                "todate": to_date
            }

            logger.info(f"Fetching historical candles with params: {historic_params}")

            candle_data = self.smart_api.getCandleData(historic_params)

            if candle_data is None:
                raise ValueError("getCandleData returned None - check session, params, or broker response")

            logger.info(f"Historical data fetched - {len(candle_data.get('data', []))} candles")

            if candle_data.get('status') == True and candle_data.get('data'):
                candles = candle_data["data"]
                if not candles:
                    self.send(text_data=json.dumps({"error": "No candles returned from broker"}))
                    return

                # Extract columns
                timestamps = [c[0] for c in candles]
                opens     = [c[1] for c in candles]
                highs     = [c[2] for c in candles]
                lows      = [c[3] for c in candles]
                closes    = [c[4] for c in candles]
                volumes   = [c[5] for c in candles]

                # Calculate indicators
                rsi = self.calculate_rsi(closes)
                macd_line, macd_signal, macd_hist = self.calculate_macd(closes)
                adx, di_plus, di_minus = self.calculate_adx(highs, lows, closes)

                # Prepare response data
                response = {
                    "status": "historical_data_with_indicators",
                    "symboltoken": symbol_token,
                    "interval": interval,
                    "num_candles": len(candles),
                    "latest_close": round(closes[-1], 2) if closes else None,
                    "rsi_14": round(rsi, 2) if rsi is not None else "Not enough data",
                    "macd": {
                        "line": round(macd_line, 4) if macd_line is not None else None,
                        "signal": round(macd_signal, 4) if macd_signal is not None else None,
                        "histogram": round(macd_hist, 4) if macd_hist is not None else None
                    },
                    "adx": {
                        "adx": round(adx, 2) if adx is not None else "Not enough data",
                        "di_plus": round(di_plus, 2) if di_plus is not None else None,
                        "di_minus": round(di_minus, 2) if di_minus is not None else None
                    },
                    # Optional: include full candles if frontend needs them
                    # "data": candles
                }

                # ────────────────────────────────────────────────
                # Run strategies (example for current symbol)
                # ────────────────────────────────────────────────
                adx_signal = self.check_adx_strategy(response["adx"], symbol_token)
                macd_signal = self.check_macd_strategy(response["macd"], symbol_token)

                # Send signals if any
                if adx_signal or macd_signal:
                    self.handle_strategy_signal(symbol_token, adx_signal or macd_signal)

                # Send the main response
                self.send(text_data=json.dumps(response))

            else:
                error_msg = candle_data.get('message', 'No historical data or failure')
                self.send(text_data=json.dumps({
                    "error": error_msg,
                    "broker_response": candle_data
                }))

        except Exception as e:
            self.send(text_data=json.dumps({
                "error": f"Historical fetch failed: {str(e)}",
                "action": "get_historical"
            }))
            logger.error(f"Historical fetch exception: {str(e)}")

    def calculate_rsi(self, closes, period=14):
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

    def calculate_macd(self, closes, fast=12, slow=26, signal=9):
        """Calculate MACD line, signal line, histogram"""
        if len(closes) < slow:
            return None, None, None
        ema_fast = pd.Series(closes).ewm(span=fast, adjust=False).mean()
        ema_slow = pd.Series(closes).ewm(span=slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        histogram = macd_line - signal_line
        return macd_line.iloc[-1], signal_line.iloc[-1], histogram.iloc[-1]

    def calculate_adx(self, highs, lows, closes, period=14):
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

    def check_adx_strategy(self, adx_info, symbol_token):
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

        current_side = self.positions.get(symbol_token, {}).get('side', 'NONE')

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

    def check_macd_strategy(self, macd_info, symbol_token):
        """
        MACD Strategy:
        - No position → BUY if MACD line > 0, SELL if MACD line < 0
        - LONG → EXIT if MACD line < 0
        - SHORT → EXIT if MACD line > 0
        """
        macd_line = macd_info.get('line')
        if macd_line is None:
            return None

        current_side = self.positions.get(symbol_token, {}).get('side', 'NONE')

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

    def handle_strategy_signal(self, symbol_token, signal):
        if signal:
            action = signal["action"]
            self.send(text_data=json.dumps({
                "status": "strategy_signal",
                "symboltoken": symbol_token,
                "signal": signal,
                "current_position": self.positions.get(symbol_token, {"side": "NONE"})
            }))
            logger.info(f"Signal generated: {action} - {signal['reason']}")

            # Simulate position update (for testing — later replace with real order)
            if action == "BUY" and self.positions.get(symbol_token, {}).get("side") == "NONE":
                self.update_position(symbol_token, "LONG", 1062.65, quantity=1)  # use real price later
                logger.info(f"Simulated BUY entry for {symbol_token}")

            elif action == "SELL" and self.positions.get(symbol_token, {}).get("side") == "NONE":
                self.update_position(symbol_token, "SHORT", 1062.65, quantity=1)

            elif action == "EXIT" and symbol_token in self.positions:
                self.close_position(symbol_token, 1062.65)  # use real latest_close              
        
    def disconnect(self, close_code):
        try:
            if self.sws:
                self.sws.close_connection()
        except:
            pass

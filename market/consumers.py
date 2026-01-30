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
from .connection import CLIENT_CODE, PIN, TOTP_SECRET, setup_connection
from .utils.indicators import calculate_rsi, calculate_macd, calculate_adx
from .utils.strategies import check_adx_strategy, check_macd_strategy
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
        threading.Thread(target=setup_connection, args=(self,), daemon=True).start()
    
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
                rsi = calculate_rsi(closes)
                macd_line, macd_signal, macd_hist = calculate_macd(closes)
                adx, di_plus, di_minus = calculate_adx(highs, lows, closes)

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
                # TEMPORARY TEST CODE - Force a BUY signal every time (for testing)
                # Remove or comment out after confirming the log appears
                # ────────────────────────────────────────────────
                # test_signal = {
                #     "action": "BUY",
                #     "reason": "Test forced BUY signal (debug mode)"
                # }
                # self.handle_strategy_signal(symbol_token, test_signal, latest_close=response["latest_close"])

                # ────────────────────────────────────────────────
                # Normal strategy checks (keep these)
                # ────────────────────────────────────────────────
                adx_signal  = check_adx_strategy(response["adx"], symbol_token)
                macd_signal = check_macd_strategy(response["macd"], symbol_token)

                # Send real signals if any (will run alongside test signal)
                for sig in [s for s in [adx_signal, macd_signal] if s]:
                    self.handle_strategy_signal(symbol_token, sig, latest_close=response["latest_close"])

                # Send the main response to frontend
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

    def handle_strategy_signal(self, symbol_token, signal, latest_close=None):
        """
        Handle strategy signal:
        - Log signal or no-signal
        - Send to frontend (strategy_signal or no_signal)
        - Simulate position update (entry/exit) using latest_close if available
        """
        entry_price = latest_close if latest_close is not None else 1062.65

        current_pos = self.positions.get(symbol_token, {"side": "NONE"})

        # Try to get symbol name from map (fallback to token)
        symbol_name = self.token_symbol_map.get(symbol_token, symbol_token)

        if signal:
            action = signal["action"]
            reason = signal["reason"]
            
            # Log the signal
            logger.info(
                f"Signal generated for {symbol_name} ({symbol_token}): "
                f"{action} - {reason} | Current position: {current_pos['side']}"
            )

            # Send signal to frontend
            self.send(text_data=json.dumps({
                "status": "strategy_signal",
                "symboltoken": symbol_token,
                "symbol": symbol_name,
                "signal": signal,
                "current_position": current_pos
            }))

            # Simulate position update (safe for testing)
            if action == "BUY" and current_pos["side"] == "NONE":
                entry_price = latest_close if latest_close is not None else 1062.65  # fallback
                self.update_position(symbol_token, "LONG", entry_price, quantity=1)
                logger.info(f"Simulated BUY entry for {symbol_name} @ {entry_price}")

            elif action == "SELL" and current_pos["side"] == "NONE":
                entry_price = latest_close if latest_close is not None else 1062.65
                self.update_position(symbol_token, "SHORT", entry_price, quantity=1)
                logger.info(f"Simulated SELL entry for {symbol_name} @ {entry_price}")

            elif action == "EXIT" and symbol_token in self.positions:
                exit_price = latest_close if latest_close is not None else 1062.65
                self.close_position(symbol_token, exit_price)
                logger.info(f"Simulated EXIT for {symbol_name} @ {exit_price} | PNL updated")

        else:
            # Log when no signal (helps debugging)
            logger.info(
                f"No signal for {symbol_name} ({symbol_token}) | "
                f"Current position: {current_pos['side']}"
            )

            # Optional: inform frontend there is no action right now
            self.send(text_data=json.dumps({
                "status": "no_signal",
                "symboltoken": symbol_token,
                "symbol": symbol_name,
                "current_position": current_pos
            }))

    def update_position(self, symbol_token, side, entry_price, quantity=1):
        """
        Create or update a position (LONG or SHORT)
        """
        self.positions[symbol_token] = {
            "side": side,               # "LONG" or "SHORT"
            "entry_price": entry_price,
            "quantity": quantity,
            "entry_time": pd.Timestamp.now(tz='Asia/Kolkata').isoformat(),
            "exchange": "NSE",          # can make dynamic later
            "target": None,
            "stoploss": None,
            "pnl": 0.0
        }
        logger.info(f"Position opened: {side} {symbol_token} @ {entry_price} (qty: {quantity})")

    def close_position(self, symbol_token, exit_price):
        """
        Close a position and calculate PNL
        """
        if symbol_token not in self.positions:
            logger.warning(f"No position to close for {symbol_token}")
            return

        pos = self.positions[symbol_token]
        if pos["side"] == "LONG":
            pnl = (exit_price - pos["entry_price"]) * pos["quantity"]
        elif pos["side"] == "SHORT":
            pnl = (pos["entry_price"] - exit_price) * pos["quantity"]
        else:
            pnl = 0.0

        pos["exit_price"] = exit_price
        pos["exit_time"] = pd.Timestamp.now(tz='Asia/Kolkata').isoformat()
        pos["pnl"] = round(pnl, 2)
        pos["mtm"] = round(pnl, 2)  # mark-to-market = realized PNL

        logger.info(f"Position closed: {symbol_token} {pos['side']} @ {exit_price} | PNL: {pnl}")

    def disconnect(self, close_code):
        try:
            if self.sws:
                self.sws.close_connection()
        except:
            pass

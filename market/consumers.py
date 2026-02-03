import json
import os
import threading
import pyotp
import requests
from dotenv import load_dotenv
import pandas as pd
import numpy as np
from channels.generic.websocket import WebsocketConsumer
from django.utils import timezone
from logzero import logger
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from .connection import CLIENT_CODE, PIN, TOTP_SECRET, setup_connection
from .utils.indicators import calculate_rsi, calculate_macd, calculate_adx
from .utils.strategies import check_adx_strategy, check_macd_strategy
from .models import Position
from timeloop import Timeloop
from datetime import timedelta

# Official exchangeType mapping from SmartAPI SDK source
EXCHANGE_MAP = {
    "NSE": 1,
    "NFO": 2,
    "BSE": 3,
    "BFO": 4,
    "MCX": 5,
    "NCO": 5,
    "NCDEX": 7,
    "CDS": 13,
}


class MarketConsumer(WebsocketConsumer):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        self.tl = Timeloop()
        self.register_timers()
        self.tl.start(block=False)

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
                self.send(json.dumps({"error": f"Unknown action: {action}"}))

        except Exception as e:
            self.send(json.dumps({"error": str(e)}))

    def handle_subscribe(self, tradingsymbols, exchange):
        if not self.instrument_list:
            return self.send(json.dumps({"error": "Instrument list not loaded"}))

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

            self.send(json.dumps({"status": "subscribed", "tradingsymbols": tradingsymbols}))

    def handle_unsubscribe(self, tradingsymbols, exchange):
        if not self.instrument_list:
            return self.send(json.dumps({"error": "Instrument list not loaded"}))

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
            self.sws.unsubscribe("abc123", 1, token_list)

            self.subscribed_tokens[exchange_type].difference_update(tokens)
            if not self.subscribed_tokens[exchange_type]:
                del self.subscribed_tokens[exchange_type]

            for token in tokens:
                if token in self.token_symbol_map:
                    del self.token_symbol_map[token]

            self.send(json.dumps({"status": "unsubscribed", "tradingsymbols": tradingsymbols}))

    def handle_place_order(self, params):
        try:
            order_response = self.smart_api.placeOrder(params)
            logger.info(f"Raw broker response from placeOrder: {order_response}")

            if order_response is None:
                error_msg = "placeOrder returned None - possible session invalid, network issue, or broker restriction"
                self.send(json.dumps({"error": error_msg, "broker_response": None}))
                logger.error(error_msg)
                return

            if isinstance(order_response, dict):
                if order_response.get('status') is True:
                    order_id = order_response.get('data', {}).get('orderid')
                    if order_id:
                        self.send(json.dumps({
                            "status": "order_placed",
                            "orderid": order_id,
                            "data": order_response
                        }))
                        logger.info(f"Order placed successfully - ID: {order_id}")
                    else:
                        self.send(json.dumps({
                            "error": "Order accepted but no orderid returned",
                            "broker_response": order_response
                        }))
                else:
                    error_msg = order_response.get('message', 'Order placement rejected by broker')
                    self.send(json.dumps({
                        "error": error_msg,
                        "errorcode": order_response.get('errorcode'),
                        "broker_response": order_response
                    }))
                    logger.error(f"Order rejected: {order_response}")
            else:
                self.send(json.dumps({
                    "error": f"Unexpected response type from placeOrder: {type(order_response)}",
                    "broker_response": str(order_response)
                }))
                logger.error(f"Unexpected type: {type(order_response)} - {order_response}")

        except Exception as e:
            self.send(json.dumps({
                "error": f"Exception during placeOrder: {str(e)}",
                "action": "place_order"
            }))
            logger.error(f"Place order exception: {str(e)}")

    def handle_modify_order(self, params):
        try:
            order_id = params.get("orderid")
            if not order_id:
                raise ValueError("orderid is required for modify")

            modify_params = {k: v for k, v in params.items() if k != "orderid"}

            response = self.smart_api.modifyOrder(order_id, modify_params)
            self.send(json.dumps({
                "status": "order_modified",
                "orderid": order_id,
                "data": response
            }))
            logger.info(f"Modify Order Success: {order_id} - {response}")
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Modify Order Failed: {error_msg}")
            self.send(json.dumps({
                "error": error_msg,
                "action": "modify_order"
            }))

    def handle_cancel_order(self, params):
        try:
            order_id = params.get("orderid")
            variety = params.get("variety", "NORMAL")

            if not order_id:
                raise ValueError("orderid is required for cancel")

            response = self.smart_api.cancelOrder(variety, order_id)
            self.send(json.dumps({
                "status": "order_canceled",
                "orderid": order_id,
                "data": response
            }))
            logger.info(f"Cancel Order Success: {order_id} - {response}")
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Cancel Order Failed: {error_msg}")
            self.send(json.dumps({
                "error": error_msg,
                "action": "cancel_order"
            }))

    def handle_get_order_details(self, params):
        try:
            order_id = params.get("orderid")
            if not order_id:
                raise ValueError("orderid is required for details")

            order_book = self.smart_api.orderBook()
            if not order_book or 'data' not in order_book:
                raise ValueError("No order book data")

            matching_order = None
            for order in order_book['data']:
                if order.get('orderid') == order_id:
                    matching_order = order
                    break

            if matching_order:
                self.send(json.dumps({
                    "status": "order_details",
                    "orderid": order_id,
                    "data": matching_order
                }))
            else:
                self.send(json.dumps({
                    "error": f"Order {order_id} not found in recent orders",
                    "action": "get_order"
                }))
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Get Order Details Failed: {error_msg}")
            self.send(json.dumps({
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
                self.send(json.dumps({
                    "status": "Re-login successful - session refreshed",
                    "auth_token_prefix": self.auth_token[:20] + "..."
                }))
                logger.info("Re-login SUCCESS")
            else:
                error_msg = login_data.get('message', 'Re-login failed - check credentials/TOTP')
                self.send(json.dumps({"error": error_msg}))
                logger.error(error_msg)

        except Exception as e:
            self.send(json.dumps({"error": f"Re-login exception: {str(e)}"}))
            logger.error(f"Re-login exception: {str(e)}")

    # ────────────────────────────────────────────────
    # Historical + Indicators + Strategy
    # ────────────────────────────────────────────────

    def handle_get_historical(self, params):
        try:
            symbol_token = params.get("symboltoken")
            exchange = params.get("exchange", "NSE")
            interval = params.get("interval", "FIFTEEN_MINUTE")
            from_date = params.get("from_date")
            to_date   = params.get("to_date")

            if not all([symbol_token, from_date, to_date]):
                raise ValueError("symboltoken, from_date, to_date are required")

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
                raise ValueError("getCandleData returned None")

            logger.info(f"Historical data fetched - {len(candle_data.get('data', []))} candles")

            if candle_data.get('status') == True and candle_data.get('data'):
                candles = candle_data["data"]
                if not candles:
                    self.send(json.dumps({"error": "No candles returned from broker"}))
                    return

                timestamps = [c[0] for c in candles]
                opens     = [c[1] for c in candles]
                highs     = [c[2] for c in candles]
                lows      = [c[3] for c in candles]
                closes    = [c[4] for c in candles]
                volumes   = [c[5] for c in candles]

                rsi = calculate_rsi(closes)
                macd_line, macd_signal, macd_hist = calculate_macd(closes)
                adx, di_plus, di_minus = calculate_adx(highs, lows, closes)

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
                }

                # Get current position from DB
                open_pos = self.get_open_position_for_token(symbol_token)
                current_side = "OPEN" if open_pos else "NONE"

                # ────────────────────────────────────────────────
                # TEMPORARY TEST: Force close if position exists
                # This helps you verify that exit_price & exit_datetime are saving
                # Remove or comment out this block after testing
                # ────────────────────────────────────────────────
                # if open_pos:
                #     # Use latest close + small offset (or any value you want to test)
                #     test_exit_price = response["latest_close"] + 5  # or -5, or fixed value like 1080
                #     logger.info(f"[TEST] Forcing exit on token {symbol_token} @ {test_exit_price}")
                #     self.close_position_db(
                #         symbol_token=symbol_token,
                #         exit_price=test_exit_price,
                #         exit_reason="TEST: Forced exit to verify saving"
                #     )
                # # ────────────────────────────────────────────────

                adx_signal  = check_adx_strategy(response["adx"], current_side)
                macd_signal = check_macd_strategy(response["macd"], current_side)

                logger.info(f"Strategy check for {symbol_token}: ADX={adx_signal}, MACD={macd_signal}, CurrentSide={current_side}")

                for sig in [s for s in [adx_signal, macd_signal] if s]:
                    self.handle_strategy_signal(symbol_token, sig, latest_close=response["latest_close"])

                self.send(json.dumps(response))

            else:
                error_msg = candle_data.get('message', 'No historical data or failure')
                self.send(json.dumps({
                    "error": error_msg,
                    "broker_response": candle_data
                }))

        except Exception as e:
            self.send(json.dumps({
                "error": f"Historical fetch failed: {str(e)}",
                "action": "get_historical"
            }))
            logger.error(f"Historical fetch exception: {str(e)}", exc_info=True)

    # ────────────────────────────────────────────────
    # Database Position Helpers
    # ────────────────────────────────────────────────

    def get_open_position_for_token(self, symbol_token):
        return Position.objects.filter(
            token=symbol_token,
            status="OPEN"
        ).order_by('-entry_datetime').first()

    def open_position_db(self, symbol_token, side, entry_price, quantity=1, lots=1):
        try:
            symbol_name = (
                self.token_symbol_map.get(symbol_token) or
                self.token_to_symbol.get(symbol_token) or
                symbol_token   # worst case fallback to token
            )
            exchange = "NSE"

            # Calculate target & stoploss (example logic)
            risk_reward = 1.5
            risk_percent = 0.01
            reward_percent = risk_percent * risk_reward

            if side == "LONG":
                target    = entry_price * (1 + reward_percent)
                stoploss  = entry_price * (1 - risk_percent)
            else:  # SHORT
                target    = entry_price * (1 - reward_percent)
                stoploss  = entry_price * (1 + risk_percent)

            position = Position.objects.create(
                symbol      = symbol_name,
                exchange    = exchange,
                token       = symbol_token,
                entry_price = entry_price,
                target      = round(target, 2),
                stoploss    = round(stoploss, 2),
                mtm         = 0.0,
                status      = "OPEN",
                
                # NEW: save lots & quantity
                lots        = lots,
                quantity    = quantity,
            )

            logger.info(f"Opened {side} {symbol_name} @ {entry_price:.2f} | "
                        f"Lots={lots} | Qty={quantity} | "
                        f"Target={target:.2f} | SL={stoploss:.2f}")

            # Send to frontend
            self.send(json.dumps({
                "status": "position_opened",
                "token": symbol_token,
                "symbol": symbol_name,
                "side": side,
                "entry_price": entry_price,
                "target": round(target, 2),
                "stoploss": round(stoploss, 2),
                "lots": lots,
                "quantity": quantity,
            }))

            return position

        except Exception as e:
            logger.error(f"Open position failed: {e}", exc_info=True)
            return None
    
    def close_position_db(self, symbol_token, exit_price, exit_reason="Strategy Exit"):
        try:
            position = self.get_open_position_for_token(symbol_token)
            if not position:
                logger.warning(f"No open position for token {symbol_token}")
                return None

            if position.entry_price is not None:
                # Use quantity for accurate PNL
                if position.entry_price < exit_price:
                    # Likely LONG (profit when price rises)
                    pnl = (exit_price - position.entry_price) * position.quantity
                else:
                    # Likely SHORT
                    pnl = (position.entry_price - exit_price) * position.quantity
            else:
                pnl = 0.0

            position.exit_price     = exit_price
            position.exit_datetime  = timezone.now()
            position.mtm            = round(pnl, 2)
            position.status         = "CLOSED"
            position.save()

            logger.info(
                f"Position CLOSED | Token={symbol_token} | "
                f"Exit price={exit_price:.2f} | "
                f"Exit time={position.exit_datetime} | "
                f"Qty={position.quantity} | MTM={position.mtm:.2f} | Reason={exit_reason}"
            )

            symbol_name = self.token_symbol_map.get(symbol_token, symbol_token)
            self.send(json.dumps({
                "status": "position_closed",
                "token": symbol_token,
                "symbol": symbol_name,
                "exit_price": round(exit_price, 2),
                "exit_datetime": position.exit_datetime.isoformat(),
                "pnl": position.mtm,
                "quantity": position.quantity,
                "reason": exit_reason
            }))

            return position

        except Exception as e:
            logger.error(f"Close position failed: {e}", exc_info=True)
            return None
    
    def handle_strategy_signal(self, symbol_token, signal, latest_close=None):
        if not signal:
            symbol_name = self.token_symbol_map.get(symbol_token, symbol_token)
            open_pos = self.get_open_position_for_token(symbol_token)

            self.send(json.dumps({
                "status": "no_signal",
                "symboltoken": symbol_token,
                "symbol": symbol_name,
                "current_position": {
                    "status": "OPEN" if open_pos else "NONE"
                }
            }))
            return

        action = signal["action"]
        reason = signal.get("reason", "No reason")
        price = latest_close if latest_close is not None else 1062.65  # fallback

        symbol_name = self.token_symbol_map.get(symbol_token, symbol_token)
        open_pos = self.get_open_position_for_token(symbol_token)

        # ── ENTRY logic ──
        if action in ("BUY", "SELL"):
            if open_pos:
                # Already have open position → don't open duplicate
                print(f"[SIGNAL IGNORE] Already {open_pos.status} position for {symbol_token} — ignoring new {action}")
                self.send(json.dumps({
                    "status": "signal_ignored",
                    "reason": "Position already open",
                    "symbol": symbol_name,
                    "token": symbol_token,
                    "current_side": "OPEN"
                }))
                return

            side = "LONG" if action == "BUY" else "SHORT"

            # TODO: Make configurable later (from frontend, risk %, etc.)
            lots = 1
            quantity = 50  # or lots * instrument_lot_size if you fetch from master

            position = self.open_position_db(
                symbol_token=symbol_token,
                side=side,
                entry_price=price,
                lots=lots,
                quantity=quantity
            )

            if position:
                self.send(json.dumps({
                    "status": "position_opened",
                    "symbol": symbol_name,
                    "token": symbol_token,
                    "side": side,
                    "entry_price": round(price, 2),
                    "target": position.target,
                    "stoploss": position.stoploss,
                    "lots": lots,
                    "quantity": quantity,
                    "signal": signal
                }))
                print(f"[ENTRY EXECUTED] {side} {symbol_name} @ {price:.2f}, Qty={quantity}")

        # ── EXIT logic ──
        elif action == "EXIT" and open_pos:
            closed_pos = self.close_position_db(
                symbol_token=symbol_token,
                exit_price=price,
                exit_reason=reason
            )

            if closed_pos:
                self.send(json.dumps({
                    "status": "position_closed",
                    "symbol": symbol_name,
                    "token": symbol_token,
                    "exit_price": round(price, 2),
                    "pnl": closed_pos.mtm,
                    "exit_datetime": closed_pos.exit_datetime.isoformat() if closed_pos.exit_datetime else None,
                    "signal": signal
                }))
                print(f"[EXIT EXECUTED] {symbol_name} closed @ {price:.2f}, PNL={closed_pos.mtm:.2f}")

        else:
            print(f"[SIGNAL UNKNOWN] Action={action} ignored for {symbol_token}")
    
    def register_timers(self):
            @self.tl.job(interval=timedelta(seconds=30))  # Run every 30 seconds — change to 60, 300, etc.
            def periodic_market_check():
                try:
                    now = timezone.now()
                    print(f"[TIMER {now.strftime('%H:%M:%S')}] Periodic check running...")

                    open_positions = Position.objects.filter(status="OPEN")

                    if open_positions.exists():
                        for pos in open_positions:
                            print(
                                f"Open: {pos.symbol} | "
                                f"Entry={pos.entry_price} | "
                                f"MTM={pos.mtm}"
                            )
                    else:
                        print("No open positions")
                        
                    if now.hour > 15 or (now.hour == 15 and now.minute >= 25):
                        for pos in open_positions:
                            exit_price = pos.entry_price  # replace with LTP
                            self.close_position_db(
                                symbol_token=pos.token,
                                exit_price=exit_price,
                                exit_reason="Market close auto square-off"
                            )
                            print(f"[AUTO SQUARE-OFF] {pos.symbol} closed")

                except Exception as e:
                    print(f"[TIMER ERROR] {e}")
                
    
    def disconnect(self, close_code):
        try:
            if self.sws:
                self.sws.close_connection()
        except:
            pass
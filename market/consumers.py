import json
import os
import threading
import pyotp
import requests
from dotenv import load_dotenv

from channels.generic.websocket import WebsocketConsumer
from logzero import logger
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# Load environment variables
load_dotenv()

API_KEY = os.getenv("ANGEL_API_KEY")
CLIENT_CODE = os.getenv("ANGEL_CLIENT_CODE")
PIN = os.getenv("ANGEL_PIN")
TOTP_SECRET = os.getenv("ANGEL_TOTP")

INSTRUMENT_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"

EXCHANGE_MAP = {
    "NSE": 1,
    "NFO": 2,
    "BSE": 3,
    "BFO": 4,
    "BCD": 5,
    "MCX": 5,
    "NCDEX": 6,
    "CDE": 7,
}


class MarketConsumer(WebsocketConsumer):

    def connect(self):
        self.accept()
        self.send(json.dumps({"status": "WebSocket Connected"}))

        self.subscribed_tokens = {}  # exchangeType: set(tokens)
        self.token_symbol_map = {}  # token: symbol (tradingsymbol)
        self.smart_api = None
        self.sws = None
        self.instrument_list = None

        # Start SmartAPI socket in background
        threading.Thread(target=self.start_smartapi, daemon=True).start()

    def start_smartapi(self):
        try:
            # -------- STEP 1: LOGIN --------
            self.smart_api = SmartConnect(api_key=API_KEY)

            totp = pyotp.TOTP(TOTP_SECRET).now()
            data = self.smart_api.generateSession(CLIENT_CODE, PIN, totp)

            auth_token = data["data"]["jwtToken"]
            feed_token = self.smart_api.getfeedToken()

            logger.info("SmartAPI Login Successful")

            # Fetch instrument list
            response = requests.get(INSTRUMENT_URL)
            self.instrument_list = response.json()

            logger.info("Instrument list fetched")

            # -------- STEP 2: WEBSOCKET --------
            self.sws = SmartWebSocketV2(
                auth_token,
                API_KEY,
                CLIENT_CODE,
                feed_token
            )

            correlation_id = "abc123"
            mode = 1  # LTP (can be made dynamic later)

            def on_open(wsapp):
                logger.info("SmartAPI WebSocket Opened")
                # Optionally subscribe to defaults here
                # self.handle_subscribe(["NIFTY", "BANKNIFTY", "FINNIFTY"], "NSE")

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
                logger.error(error)
                self.send(text_data=json.dumps({
                    "error": str(error)
                }))

            def on_close(wsapp):
                logger.info("SmartAPI WebSocket Closed")

            self.sws.on_open = on_open
            self.sws.on_data = on_data
            self.sws.on_error = on_error
            self.sws.on_close = on_close

            self.sws.connect()

        except Exception as e:
            logger.error(e)
            self.send(text_data=json.dumps({
                "error": str(e)
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
            self.send(text_data=json.dumps({"status": "order_placed", "data": order_response}))
        except Exception as e:
            self.send(text_data=json.dumps({"error": str(e)}))

    def disconnect(self, close_code):
        try:
            self.sws.close_connection()
        except:
            pass

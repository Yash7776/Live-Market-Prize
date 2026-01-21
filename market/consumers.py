import json
import os
import threading
import pyotp
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


class MarketConsumer(WebsocketConsumer):

    def connect(self):
        self.accept()
        self.send(json.dumps({"status": "WebSocket Connected"}))

        # Start SmartAPI socket in background
        threading.Thread(target=self.start_smartapi, daemon=True).start()

    def start_smartapi(self):
        try:
            # -------- STEP 1: LOGIN --------
            smart_api = SmartConnect(api_key=API_KEY)

            totp = pyotp.TOTP(TOTP_SECRET).now()
            data = smart_api.generateSession(CLIENT_CODE, PIN, totp)

            auth_token = data["data"]["jwtToken"]
            feed_token = smart_api.getfeedToken()

            logger.info("SmartAPI Login Successful")

            # -------- STEP 2: WEBSOCKET --------
            self.sws = SmartWebSocketV2(
                auth_token,
                API_KEY,
                CLIENT_CODE,
                feed_token
            )

            correlation_id = "abc123"
            mode = 1  # LTP

            token_list = [
                {
                    "exchangeType": 1,  # NSE
                    "tokens": ["26009"]  # NIFTY
                }
            ]

            def on_open(wsapp):
                logger.info("SmartAPI WebSocket Opened")
                self.sws.subscribe(correlation_id, mode, token_list)

            def on_data(wsapp, message):
                ltp = message.get("last_traded_price")

                if ltp:
                    price = ltp / 100

                    self.send(text_data=json.dumps({
                        "symbol": "NIFTY",
                        "ltp": price,
                        "timestamp": message.get("exchange_timestamp")
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

    def disconnect(self, close_code):
        try:
            self.sws.close_connection()
        except:
            pass

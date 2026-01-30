# market/connection.py
import json
import os
import threading
import pyotp
import requests
from dotenv import load_dotenv
from logzero import logger
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

load_dotenv()

# Load the same constants here (safe duplication for now)
API_KEY      = os.getenv("ANGEL_API_KEY")
CLIENT_CODE  = os.getenv("ANGEL_CLIENT_CODE")
PIN          = os.getenv("ANGEL_PIN")
TOTP_SECRET  = os.getenv("ANGEL_TOTP")
INSTRUMENT_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"

def setup_connection(consumer):
    try:
        consumer.smart_api = SmartConnect(api_key=API_KEY)

        totp_code = pyotp.TOTP(TOTP_SECRET).now()
        logger.info(f"Generated TOTP: {totp_code}")

        login_data = consumer.smart_api.generateSession(CLIENT_CODE, PIN, totp_code)

        if login_data.get('status') == False:
            error_msg = login_data.get('message', 'Login failed')
            logger.error(error_msg)
            consumer.send(json.dumps({"error": error_msg, "login_status": "FAILED"}))
            return

        consumer.auth_token = login_data["data"]["jwtToken"]
        consumer.feed_token = consumer.smart_api.getfeedToken()

        logger.info(f"Login SUCCESS - Auth Token: {consumer.auth_token[:20]}... | Feed Token: {consumer.feed_token[:20]}...")
        consumer.send(json.dumps({
            "status": "Login Successful",
            "login_status": "SUCCESS",
            "client_code": CLIENT_CODE
        }))

        # Fetch instrument list
        response = requests.get(INSTRUMENT_URL)
        if response.status_code == 200:
            consumer.instrument_list = response.json()
            logger.info(f"Instrument list fetched - {len(consumer.instrument_list)} entries")
        else:
            logger.error("Failed to fetch instrument list")
            consumer.send(json.dumps({"error": "Instrument list download failed"}))

        # Start WebSocket Datafeed
        consumer.sws = SmartWebSocketV2(
            consumer.auth_token,
            API_KEY,
            CLIENT_CODE,
            consumer.feed_token
        )

        correlation_id = "market_stream_001"
        mode = 1  # LTP

        def on_open(wsapp):
            logger.info("SmartAPI WebSocket Opened - Ready for subscriptions")
            consumer.send(json.dumps({"status": "Datafeed Connected"}))

        def on_data(wsapp, message):
            token = message.get("token")
            ltp = message.get("last_traded_price")

            if not ltp:
                return

            price = ltp / 100
            symbol = consumer.token_symbol_map.get(token, "UNKNOWN")

            consumer.send(text_data=json.dumps({
                "symbol": symbol,
                "token": token,
                "ltp": price
            }))

        def on_error(wsapp, error):
            logger.error(f"WebSocket Error: {error}")
            consumer.send(text_data=json.dumps({"error": str(error)}))

        def on_close(wsapp):
            logger.info("SmartAPI WebSocket Closed - Attempting reconnect in 5 seconds...")
            consumer.send(text_data=json.dumps({"status": "Datafeed Disconnected - Reconnecting..."}))

            import time
            time.sleep(5)
            try:
                logger.info("Reconnecting WebSocket...")
                consumer.sws.connect()
            except Exception as e:
                logger.error(f"Reconnect failed: {e}")
                consumer.send(text_data=json.dumps({"error": f"Reconnect failed: {str(e)}"}))

        consumer.sws.on_open = on_open
        consumer.sws.on_data = on_data
        consumer.sws.on_error = on_error
        consumer.sws.on_close = on_close

        consumer.sws.connect()

    except Exception as e:
        logger.error(f"Critical error in connection setup: {str(e)}")
        consumer.send(json.dumps({
            "error": f"Login/Datafeed failed: {str(e)}",
            "login_status": "FAILED"
        }))
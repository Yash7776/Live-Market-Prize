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

from market.models import Position

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
            consumer.token_to_symbol = {}
            for instr in consumer.instrument_list:
                token = instr.get("token")
                ts = instr.get("symbol")  # this is "SBIN-EQ", "NIFTY50", etc.
                if token and ts:
                    consumer.token_to_symbol[token] = ts
            logger.info(f"Created token-to-symbol map with {len(consumer.token_to_symbol)} entries")
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
            ltp_raw = message.get("last_traded_price")
            
            if not ltp_raw:
                print(f"[LTP DEBUG] No LTP in message for token {token}")
                return

            ltp = ltp_raw / 100
            symbol = consumer.token_symbol_map.get(token, "UNKNOWN")

            # Print every incoming LTP (proof that live data is flowing)
            print(f"[LTP RECEIVED] {symbol} ({token}) → LTP = {ltp:.2f}")

            # 1. Always send current LTP to frontend
            consumer.send(json.dumps({
                "symbol": symbol,
                "token": token,
                "ltp": round(ltp, 2)
            }))

            # 2. Try to update MTM and check target/stoploss for open positions
            try:
                print(f"[POSITION CHECK] Looking for OPEN position with token {token}")
                open_position = Position.objects.filter(
                    token=token,
                    status="OPEN"
                ).first()

                if open_position and open_position.entry_price is not None:
                    entry = open_position.entry_price
                    qty = open_position.quantity

                    print(f"[POSITION FOUND] ID={open_position.id} | {symbol} | Entry={entry:.2f} | Qty={qty} | Current MTM={open_position.mtm:.2f}")

                    # Direction-aware MTM calculation
                    if ltp >= entry:
                        mtm = (ltp - entry) * qty
                        dir_guess = "LONG"
                    else:
                        mtm = (entry - ltp) * qty
                        dir_guess = "SHORT"

                    print(f"[MTM CALC] LTP={ltp:.2f} | Direction guess={dir_guess} | New MTM={mtm:.2f} | Old MTM={open_position.mtm:.2f}")

                    # Check if change is significant
                    change = abs(mtm - open_position.mtm)
                    print(f"[MTM CHANGE] Difference = {change:.2f}")

                    if change > 0.01:  # lowered threshold for easier testing
                        open_position.mtm = round(mtm, 2)
                        open_position.save(update_fields=['mtm'])
                        print(f"[MTM SAVED] New value saved to DB = {open_position.mtm:.2f}")

                        # Send MTM update to frontend
                        consumer.send(json.dumps({
                            "status": "mtm_update",
                            "token": token,
                            "symbol": symbol,
                            "ltp": round(ltp, 2),
                            "mtm": open_position.mtm,
                            "entry_price": round(entry, 2),
                            "direction_guess": dir_guess
                        }))
                    else:
                        print("[MTM SKIPPED] Change too small – not saving")

                    # 3. Check if target or stoploss hit → auto exit
                    exit_reason = None
                    should_exit = False

                    print(f"[EXIT CHECK] Target={open_position.target}, Stoploss={open_position.stoploss}")

                    # Target hit?
                    if open_position.target is not None:
                        if (ltp >= open_position.target * 0.99 and ltp >= entry) or \
                        (ltp <= open_position.target * 1.01 and ltp <= entry):
                            exit_reason = "Target reached"
                            should_exit = True
                            print(f"[EXIT TRIGGER] Target hit condition met → LTP={ltp:.2f}")

                    # Stoploss hit?
                    if open_position.stoploss is not None:
                        if (ltp <= open_position.stoploss and ltp <= entry) or \
                        (ltp >= open_position.stoploss and ltp >= entry):
                            exit_reason = "Stoploss hit"
                            should_exit = True
                            print(f"[EXIT TRIGGER] Stoploss hit condition met → LTP={ltp:.2f}")

                    if should_exit:
                        print(f"[AUTO EXIT START] Reason={exit_reason} | Price={ltp:.2f}")
                        consumer.close_position_db(
                            symbol_token=token,
                            exit_price=ltp,
                            exit_reason=exit_reason
                        )
                        logger.info(f"AUTO EXIT | {exit_reason} | {symbol} ({token}) | Price={ltp:.2f}")

                        # Notify frontend
                        consumer.send(json.dumps({
                            "status": "auto_exit",
                            "token": token,
                            "symbol": symbol,
                            "exit_price": round(ltp, 2),
                            "exit_reason": exit_reason,
                            "mtm": open_position.mtm
                        }))
                        print(f"[AUTO EXIT FINISH] Position closed")
                    else:
                        print("[EXIT CHECK] No exit condition met")

                else:
                    print(f"[POSITION NOT FOUND] No OPEN position for token {token}")

            except Exception as e:
                print(f"[ERROR in on_data] Token={token} | Error: {e}")
                logger.error(f"Error in on_data processing for token {token}: {e}", exc_info=True)

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
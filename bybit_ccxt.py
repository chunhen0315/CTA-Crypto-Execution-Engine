"""
Pionex_execution_ccxt.py
"""
import os
from datetime import datetime, timedelta
import ccxt.async_support as ccxt
import requests
import sys
from dotenv import load_dotenv
import config_exe
from logger import setup_logger 
import asyncio

SCRIPT_NAME = os.path.basename(__file__).replace(".py", "")
logger = setup_logger(f"{SCRIPT_NAME}.log")

load_dotenv()  
previous_wallet_balance = None  # To track wallet balance changes

KILL_SWITCH_BALANCE = config_exe.KILL_SWITCH_BALANCE
# ORDER_REFRESH_INTERVAL = config_exe.ORDER_REFRESH_INTERVAL
MODE = config_exe.MODE


def send_notification(message):
    base_url = os.getenv("base_url")
    if not base_url:
        logger.info("Notification skipped: base_url is not configured in .env")
        return

    try:
        requests.get(base_url + "\n" + message, timeout=10)
    except Exception as e:
        logger.warning(f"Notification failed: {e}")


async def initialize_exchange():
    print(f"Initiating {MODE} mode")
    if MODE == "live":
        exchange = ccxt.bybit({
            'apiKey': os.getenv('BYBIT_API_KEY'),
            'secret': os.getenv('BYBIT_API_SECRET'),
            'enableRateLimit': True,
            'options': {
                'defaultType': 'linear',  # or 'spot' / 'inverse'
            }
        })

    elif MODE == 'testnet':
        exchange = ccxt.bybit({
            'apiKey': os.getenv('BYBIT_TESTNET_API_KEY'),
            'secret': os.getenv('BYBIT_TESTNET_API_SECRET'),
            'enableRateLimit': True,
            'options': {
                'defaultType': 'linear',  # or 'spot' / 'inverse'
            }
        })
        exchange.set_sandbox_mode(True)
    else:
        raise ValueError(f"Invalid MODE: {MODE}. Must be 'live' or 'testnet'")

    return exchange


async def fetch_wallet_balance(exchange):
    try:
        balance = await exchange.fetch_balance()
        return balance['total']['USDT']
    except Exception as e:
        logger.warning(f"Error fetching wallet balance: {e}")
        return None


async def fetch_position_size(exchange):
    try:
        symbol = config_exe.symbol
        positions = await exchange.fetch_positions()

        for position in positions:
            if position['symbol'] == symbol:
                size = float(position['info']['size'])
                if position['side'] == 'short':
                    size = -size  # Convert size to negative for sell positions
                # print(f"Current position size for {symbol}: {size}")
                return size

        # If no position found for the symbol
        logger.info(f"No position found for {symbol}")
        return 0.0

    except Exception as e:
        logger.warning(f"Error fetching position: {e}")
        return None


async def process_trade_details(exchange, symbol):
    global previous_wallet_balance

    try:
        # Fetch the latest trade details
        trades = await exchange.fetch_my_trades(symbol)
        latest_trade = trades[-1]

        # Format order details
        order_details = {
            "datetime": latest_trade["datetime"],
            "symbol": latest_trade["symbol"],
            "type": latest_trade["type"],
            "side": latest_trade["side"],
            "price": latest_trade["price"],
            "amount": latest_trade["amount"],
            "cost": latest_trade["cost"],
            "fee": latest_trade["fee"]["cost"],
        }

        # Parse the original datetime and add 8 hours
        original_datetime = datetime.strptime(order_details['datetime'], '%Y-%m-%dT%H:%M:%S.%fZ')
        new_datetime = original_datetime + timedelta(hours=8)

        # Calculate PnL
        pnl = None
        new_wallet_balance = await fetch_wallet_balance(exchange)
        if new_wallet_balance is not None and previous_wallet_balance is not None:
            pnl = new_wallet_balance - previous_wallet_balance

        # Update the previous wallet balance
        previous_wallet_balance = new_wallet_balance

        # Construct message with order details and other relevant information
        message = (
            f"datetime: {new_datetime.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"action: {latest_trade['side']}\n"  # Display the passed action
            f"symbol: {order_details['symbol']}\n"
            f"type: {order_details['type']}\n"
            f"price: {order_details['price']}\n"
            f"amount: {abs(order_details['amount'])} {symbol.split('/')[0]}\n"
            f"cost: {round(order_details['cost'], 2)} {symbol.split('/')[1]}\n"
            f"fee: {round(order_details['fee'], 2)} {symbol.split('/')[1]}\n"
            f"New Position: {await fetch_position_size(exchange)} {symbol.split('/')[0]}\n"
            f"Wallet: {round(new_wallet_balance, 2)} USDT\n"
            f"PnL: {round(pnl, 2) if pnl is not None else 'N/A'} USDT\n"
            "*************************************"
        )

        logger.info(message)
        send_notification(message)

    except Exception as e:
        logger.warning(f"Error processing trade details: {e}")


async def execute_market(exchange, net_pos):
    global previous_wallet_balance

    try:
        # Fetch current wallet balance before the trade
        current_wallet_balance = await fetch_wallet_balance(exchange)
        if current_wallet_balance is None:
            logger.warning("Unable to fetch wallet balance. Trade execution aborted.")
            return

        position_size = await fetch_position_size(exchange)
        if position_size is None:
            logger.warning("Error fetching position size. Check logs for details.")
            return

        difference = net_pos - position_size
        # print(f"Difference between net_pos and current position: {difference:.3f}")

        order = None
        symbol = config_exe.symbol

        if difference > 0:
            # Market buy order
            order = await exchange.create_market_buy_order(symbol, abs(difference))
            logger.info(f"Market buy order executed: {order['id']}")

        elif difference < 0:
            # Market sell order
            order = await exchange.create_market_sell_order(symbol, abs(difference))
            logger.info(f"Market sell order executed: {order['id']}")

        else:
            # print("No trade needed. Position size difference is zero.")
            return None

        if order:
            # Call process_trade_details with action and trade details
            await process_trade_details(exchange, symbol)
        return order

    except Exception as e:
        logger.warning(f"Error executing trade: {e}")
        return None


async def check_kill_switch(exchange):
    try:
        # Fetch wallet balance
        balance = await exchange.fetch_balance()

        # Check wallet balance in USDT
        wallet_balance = balance['total']['USDT']

        if wallet_balance < KILL_SWITCH_BALANCE:
            logger.warning("Wallet balance is below kill switch threshold. Closing positions and cancelling orders...")

            # Fetch current positions
            positions = await exchange.fetch_positions()

            # Close all positions
            for position in positions:
                size = float(position['info']['size'])
                if size != 0:
                    if position['side'] == 'long':
                        await exchange.create_market_sell_order(position['symbol'], size)
                        logger.info(f"Closed long position for {position['symbol']}")
                    elif position['side'] == 'short':
                        await exchange.create_market_buy_order(position['symbol'], abs(size))
                        logger.info(f"Closed short position for {position['symbol']}")

            # Cancel all pending orders
            orders = await exchange.fetch_open_orders(config_exe.symbol)
            for order in orders:
                await exchange.cancel_order(order['id'])
                logger.info(f"Cancelled order {order['id']}")

            logger.info("Kill switch activated. Bot stopped.")
            send_notification("Kill switch activated. Bot stopped.")
            sys.exit(1)  # Exit the script with status code 1 (indicating abnormal termination)

        return False

    except Exception as e:
        logger.warning(f"Error in kill switch: {e}")
        return False


# Asynchronous main function to test the rest of the functions
async def main():
    exchange = None
    try:
        # Initialize exchange
        exchange = await initialize_exchange()
        if exchange:
            logger.info("Exchange initialized successfully.")
        else:
            logger.warning("Failed to initialize exchange.")
            return

        # Fetch and print wallet balance
        if exchange:
            wallet_balance = await fetch_wallet_balance(exchange)
            if wallet_balance is not None:
                logger.info(f"Wallet balance: {wallet_balance} USDT")
            else:
                logger.warning("Failed to fetch wallet balance")
        else:
            logger.warning("Exchange not initialized, skipping wallet balance fetch")

        # Fetch and print current position size
        if exchange:
            position_size = await fetch_position_size(exchange)
            logger.info(f"Position size: {position_size}")

        # Test executing a market order
        if exchange:
            logger.info("Testing market order execution...")
            await execute_market(exchange, net_pos=-0.001)

        # Test the kill switch
        if exchange:
            print("Testing kill switch...")
            kill_switch_activated = await check_kill_switch(exchange)
            logger.info(f"Kill switch activated: {kill_switch_activated}")

    except Exception as e:
        logger.warning(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # 确保释放所有资源
        if exchange:
            await exchange.close()
            logger.info("Exchange connection closed.")


# Run the main function
# asyncio.run(main())

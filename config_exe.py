MODE = "testnet"  # paper, testnet, live

base = "BTC"
quote = "USDT"
# symbol = base + quote
symbol = f"{base}/{quote}:USDT"

# ORDER SIZE
single_pos = 0.001 # single btc order
# max_pos = 0.03  # max amount of btc
# min_pos = -0.03  # min amount of btc

# Paper trading
PAPER_INITIAL_BALANCE = 10000.0
PAPER_FEE_RATE = 0.00035
PAPER_MAX_PRICE_AGE_HOURS = 6
PRICE_FRESHNESS_WAIT_TIMEOUT = 600
PRICE_FRESHNESS_POLL_SECONDS = 300

# Kill the bot if balance falls below this amount
KILL_SWITCH_BALANCE = 100.0

MAIN_REFRESH_INTERVAL = 300  # 5 minutes
SIGNAL_MAX_WAIT_MINUTES = 45
SIGNAL_DEADLINE_TOLERANCE_SECONDS = 30
MIN_FRESH_WEIGHT = 0.70
EXECUTE_EARLY_WHEN_FULLY_ALIGNED = True
RENORMALIZE_FRESH_SIGNALS = False
CONTINUOUS_DELAY_ALERT_HOURS = 3
DAILY_DELAY_ALERT_COUNT = 5
DELAY_ALERT_TIMEZONE = "Asia/Kuala_Lumpur"
# for limit order
# ORDER_REFRESH_INTERVAL = 10

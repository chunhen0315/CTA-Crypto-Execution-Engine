"""
Topic list
GLASSNODE,CRYPTOQUANT BTC, 1h
"""
import os
from dotenv import load_dotenv
from datetime import datetime, timezone
import pandas as pd

# 加载.env文件
load_dotenv()

API_KEY = os.getenv('CYBOTRADE_API_KEY')


def utc_datetime(value):
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


ASSET = "BTC"
SYMBOL = f"{ASSET}/USDT"
INTERVAL = "1h"
PRICE_SOURCE_INTERVAL = "1m"
PRICE_OUTPUT_INTERVAL = INTERVAL
# Default alpha price shift. Datafeed price files stay unshifted; alpha_lib.py applies this per alpha.
PRICE_SHIFT_CANDLE_MINUTE = -60
DATA_LEN = 10000
DATA_SNAPSHOT_RETENTION_DAYS = 3
DATA_REVISION_TOLERANCE = 1e-7
DATA_REVISION_TOLERANCE_PERCENT = 1.0
PRICE_DATA_LEN_HOURS = 720  # 30 days of 1m price; keeps server memory usage sane.
START_TIME = utc_datetime("2021-01-01 00:00:00")
ALPHA_MODEL_START_TIME = START_TIME
ALPHA_SIGNAL_START_TIME = START_TIME
ALPHA_SIGNAL_END_TIME = None

REFRESH_INTERVAL = 1  # 正常刷新间隔（单位：hour）
RE_REFRESH_INTERVAL = 60 * 1  # 主动刷新间隔（单位：second）
MAX_TIME_DIFFERENCE = 150  # 最大时间差（单位：minute）
DELAYED_DATA_RETRY_MINUTES = 5
DATA_RETRY_DEADLINE_MINUTE = 45

CQ_HOUR_DIFF = 2
CQ_DELAY_COLLECT = 5  # minute
GN_DELAY_COLLECT = 10  # minute
PRICE_DELAY_COLLECT = 4


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DATA_PATH = os.path.join(BASE_DIR, "data")

PRICE_FOLDER_PATH = os.path.join(BASE_DATA_PATH, "price")
GN_FOLDER_PATH = os.path.join(BASE_DATA_PATH, "GN_BTC_1h")
CQ_FOLDER_PATH = os.path.join(BASE_DATA_PATH, "CQ_BTC_1h")
ALPHA_SIGNAL_PATH = os.path.join(BASE_DATA_PATH, "alpha_signals")
PORTFOLIO_EXECUTION_FILE = os.path.join(BASE_DATA_PATH, "portfolio_execution.csv")
ALPHA_SIGNAL_RECEIPT_STATE_FILE = os.path.join(
    BASE_DATA_PATH,
    "live_monitor",
    "alpha_signal_receipts.json",
)
ALPHA_SIGNAL_RECEIPT_RETENTION_DAYS = 30

# Alphas enabled for live signal calculation and data-topic validation.
ACTIVE_ALPHAS = [
    "alpha080",
    "alpha142",
    "alpha008",
    "alpha078",
    "alpha072",
    "alpha039",
    "alpha090",
    "alpha015",
    "alpha069",
]


GN_BTC1h_topic = [
    # "glassnode|derivatives/options_25delta_skew_6_months?a=BTC&e=deribit&i=1h",
    # "glassnode|derivatives/futures_open_interest_sum?a=BTC&e=binance&i=1h",
    # "glassnode|blockchain/utxo_loss_count?a=BTC&i=1h",
    # "glassnode|fees/exchanges_relative?a=BTC&i=1h",
    # "glassnode|fees/volume_sum?a=BTC&i=1h",
    # "glassnode|blockchain/utxo_profit_relative?a=BTC&i=1h",
    # "glassnode|fees/exchanges_mean?a=BTC&i=1h",
    # "glassnode|blockchain/utxo_created_count?a=BTC&i=1h",
    # "glassnode|market/realized_volatility_3_months?a=BTC&i=1h",
    # "glassnode|supply/inflation_rate?a=BTC&i=1h",
    # "glassnode|supply/loss_sum?a=BTC&i=1h",
    # "glassnode|derivatives/futures_volume_daily_sum_all?a=BTC&i=1h",
    # "glassnode|transactions/transfers_volume_entity_adjusted_from_lth_sth_profit_loss_relative?a=BTC&i=1h",
    # "glassnode|mining/revenue_from_fees?a=BTC&i=1h",
    # "glassnode|market/deltacap_usd?a=BTC&i=1h",
    # "glassnode|mempool/fees_sum?a=BTC&i=1h",
    # "glassnode|market/realized_volatility_1_year?a=BTC&i=1h",
    # "glassnode|market/realized_volatility_6_months?a=BTC&i=1h",
    # "glassnode|mempool/fees_sum?a=BTC&i=1h",
    # "glassnode|market/realized_volatility_all?a=BTC&i=1h",
    "glassnode|derivatives/futures_funding_rate_perpetual_all?a=BTC&i=1h",
    # "glassnode|indicators/unrealized_loss?a=BTC&i=1h",
    "glassnode|indicators/unrealized_loss_more_155?a=BTC&i=1h",
    # "glassnode|transactions/transfers_volume_exchanges_net_by_size?a=BTC&e=binance&i=1h",
    # "glassnode|indicators/cdd_sth_account_based?a=BTC&i=1h",
    # "glassnode|transactions/transfers_volume_lth_to_exchanges_sum?a=BTC&e=binance&i=1h",
    # "glassnode|mining/thermocap?a=BTC&i=1h",
    # "glassnode|indicators/net_unrealized_profit_loss?a=BTC&i=1h",
    # "glassnode|distribution/balance_exchanges_relative?a=BTC&e=binance&i=1h",
    # "glassnode|fees/exchanges_mean_pit?a=BTC&i=1h",
    # "glassnode|supply/issued?a=BTC&i=1h",
    # "glassnode|derivatives/options_atm_implied_volatility_1_week?a=BTC&e=deribit&i=1h",
    # "glassnode|transactions/transfers_volume_sum?a=BTC&i=1h",
    # "glassnode|indicators/ssr?a=BTC&i=1h",
    # "glassnode|supply/revived_more_2y_sum?a=BTC&i=1h",

]

CQ_BTC1h_topic = [
    "cryptoquant|btc/market-data/coinbase-premium-index?window=hour",
    "cryptoquant|btc/network-indicator/spent-output-supply-distribution?window=hour",
    "cryptoquant|btc/flow-indicator/exchange-whale-ratio?exchange=binance&window=hour",
    "cryptoquant|btc/market-data/funding-rates?exchange=binance&window=hour",
    # "cryptoquant|btc/flow-indicator/exchange-inflow-age-distribution?exchange=all_exchange&window=hour",
    # "cryptoquant|btc/exchange-flows/outflow?exchange=binance&window=hour",
    "cryptoquant|btc/network-indicator/stock-to-flow?window=hour",
    "cryptoquant|btc/network-data/addresses-count?window=hour",
    # "cryptoquant|btc/miner-flows/transactions-count?miner=f2pool&window=hour",
    "cryptoquant|btc/network-indicator/spent-output-age-distribution?window=hour",
    # "cryptoquant|btc/network-data/blockreward?window=hour",
    "cryptoquant|btc/exchange-flows/transactions-count?exchange=binance&window=hour",
    # "cryptoquant|btc/exchange-flows/in-house-flow?exchange=binance&window=hour",
    "cryptoquant|btc/network-data/transactions-count?window=hour",
    # "cryptoquant|btc/network-data/tokens-transferred?window=hour",
    # "cryptoquant|btc/miner-flows/inflow?miner=f2pool&window=hour",
    # "cryptoquant|btc/flow-indicator/exchange-inflow-age-distribution?exchange=all_exchange&window=hour",
    # "cryptoquant|btc/network-data/supply?window=hour",
    # "cryptoquant|btc/miner-flows/addresses-count?miner=f2pool&window=hour",
    # "cryptoquant|btc/network-indicator/utxo-age-distribution?window=hour",
    "cryptoquant|btc/market-data/liquidations?exchange=binance&window=hour",
    "cryptoquant|btc/exchange-flows/netflow?exchange=binance&window=hour",
    # "cryptoquant|btc/exchange-flows/reserve?exchange=binance&window=hour",
    # "cryptoquant|btc/market-indicator/utxo-realized-price-age-distribution?window=hour",
    "cryptoquant|btc/market-indicator/estimated-leverage-ratio?exchange=binance&window=hour",
    "cryptoquant|btc/market-data/taker-buy-sell-stats?exchange=binance&window=hour",
    # "cryptoquant|btc/miner-flows/outflow?miner=f2pool&window=hour",
    # "cryptoquant|btc/miner-flows/netflow?miner=f2pool&window=hour",
    # "cryptoquant|btc/flow-indicator/exchange-inflow-age-distribution?exchange=all_exchange&window=hour",
    "cryptoquant|btc/network-data/fees?window=hour",
    "cryptoquant|btc/network-data/fees-transaction?window=hour",
    "cryptoquant|btc/market-data/open-interest?exchange=binance&window=hour",
    # "cryptoquant|btc/network-indicator/utxo-count-age-distribution?window=hour",
    # "cryptoquant|btc/flow-indicator/exchange-inflow-supply-distribution?exchange=binance&window=hour",
    # "cryptoquant|btc/network-indicator/utxo-age-distribution?window=hour"
]

PRICE_topic = [
    "bybit-linear|candle?interval=1m&symbol=BTCUSDT",
    # "bybit-linear|candle?interval=1m&symbol=ETHUSDT",
]

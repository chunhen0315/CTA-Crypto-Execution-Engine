import os
import time
import uuid
from lib import transformation_lib, models_lib, entry_exit_logic_lib
import pandas as pd
import config_data
import numpy as np
import warnings
warnings.filterwarnings('ignore', category=FutureWarning)


GN_DATA_FILEPATH = config_data.GN_FOLDER_PATH
CQ_DATA_FILEPATH = config_data.CQ_FOLDER_PATH
ALPHA_SIGNAL_PATH = config_data.ALPHA_SIGNAL_PATH
PRICE_FILEPATH = config_data.PRICE_FOLDER_PATH
LIVE_MONITOR_PATH = os.path.join(config_data.BASE_DATA_PATH, "live_monitor")
LATEST_SIGNAL_STATE_FILE = os.path.join(LIVE_MONITOR_PATH, "latest_signal_state.csv")

pd.set_option('display.max_columns', None)  # 显示所有列


def read_required_parquet(file_path, required_columns):
    if isinstance(required_columns, str):
        required_columns = [required_columns]

    columns = ['datetime', *required_columns]
    df = pd.read_parquet(file_path, columns=columns).set_index('datetime')
    return df.dropna(subset=required_columns)


def slice_alpha_output(df):
    df = df.copy()
    df.index = pd.to_datetime(df.index, utc=True)

    start_time = getattr(config_data, "ALPHA_SIGNAL_START_TIME", None)
    end_time = getattr(config_data, "ALPHA_SIGNAL_END_TIME", None)

    if start_time is not None:
        start_time = pd.to_datetime(start_time, utc=True)
        df = df[df.index >= start_time]
    if end_time is not None:
        end_time = pd.to_datetime(end_time, utc=True)
        df = df[df.index <= end_time]

    return df


def slice_alpha_model_input(df):
    start_time = getattr(config_data, "ALPHA_MODEL_START_TIME", None)
    if start_time is None:
        return df

    df = df.copy()
    df.index = pd.to_datetime(df.index, utc=True)
    start_time = pd.to_datetime(start_time, utc=True)
    return df[df.index >= start_time]


def alpha008():
    alpha_id = "alpha008"
    file_path1 = os.path.join(CQ_DATA_FILEPATH, 'btc_market-data_coinbase-premium-index_BTC_1h.parquet')
    file_path2 = os.path.join(CQ_DATA_FILEPATH, 'btc_flow-indicator_exchange-whale-ratio_BTC_1h.parquet')
    asset = "BTC"
    transform = "None"
    model = "ezscorev1"
    logic = "trend"
    side = "both"
    window = 330
    threshold_1 = 0.85
    threshold_2 = -0.9
    price_shift_candle_minute = -60

    factor_1 = read_required_parquet(file_path1, 'coinbase_premium_index').rename(
        columns={'coinbase_premium_index': 'factor_1'})
    factor_1['factor_1'] = pd.to_numeric(factor_1['factor_1'], errors='coerce')
    factor_2 = read_required_parquet(file_path2, 'exchange_whale_ratio').rename(
        columns={'exchange_whale_ratio': 'factor_2'})
    factor_2['factor_2'] = pd.to_numeric(factor_2['factor_2'], errors='coerce')

    df = pd.merge(factor_1, factor_2, how='inner', left_index=True, right_index=True)
    df = df.sort_index()
    df['data'] = df['factor_1'] * df['factor_2']
    df = df[['factor_1', 'factor_2', 'data']]
    df = process_data(
        df,
        transform,
        model,
        logic,
        side,
        window,
        threshold_1,
        threshold_2,
        asset,
        price_shift_candle_minute=price_shift_candle_minute,
    )
    df.index.name = 'time'
    append_df_to_csv(df, f"{alpha_id}.csv")
    return df


def alpha015():
    alpha_id = "alpha015"
    file_path1 = os.path.join(CQ_DATA_FILEPATH, 'btc_market-data_funding-rates_BTC_1h.parquet')
    file_path2 = os.path.join(CQ_DATA_FILEPATH, 'btc_network-data_addresses-count_BTC_1h.parquet')
    asset = "BTC"
    transform = "None"
    model = "cciv2_zscore"
    logic = "mr_reverse"
    side = "both"
    window = 560
    threshold_1 = 2
    threshold_2 = -1.6
    price_shift_candle_minute = -60

    factor_1 = read_required_parquet(file_path1, 'funding_rates').rename(
        columns={'funding_rates': 'factor_1'})
    factor_1['factor_1'] = pd.to_numeric(factor_1['factor_1'], errors='coerce')
    factor_2 = read_required_parquet(file_path2, 'addresses_count_active').rename(
        columns={'addresses_count_active': 'factor_2'})
    factor_2['factor_2'] = pd.to_numeric(factor_2['factor_2'], errors='coerce')

    df = pd.merge(factor_1, factor_2, how='inner', left_index=True, right_index=True)
    df = df.sort_index()
    df['data'] = df['factor_1'] / df['factor_2']
    df = df[['factor_1', 'factor_2', 'data']]
    df = process_data(
        df,
        transform,
        model,
        logic,
        side,
        window,
        threshold_1,
        threshold_2,
        asset,
        price_shift_candle_minute=price_shift_candle_minute,
    )
    df.index.name = 'time'
    append_df_to_csv(df, f"{alpha_id}.csv")
    return df


def alpha039():
    alpha_id = "alpha039"
    file_path1 = os.path.join(GN_DATA_FILEPATH, 'derivatives_futures_funding_rate_perpetual_all_BTC_1h.parquet')
    file_path2 = os.path.join(GN_DATA_FILEPATH, 'indicators_unrealized_loss_more_155_BTC_1h.parquet')
    asset = "BTC"
    transform = "None"
    model = "percentilerank_meannorm"
    logic = "fast_reverse"
    side = "both"
    window = 770
    threshold_1 = 0.7
    threshold_2 = -0.25
    price_shift_candle_minute = -70

    factor_1_raw = read_required_parquet(file_path1, 'o')
    factor_1 = factor_1_raw['o'].apply(lambda value: value.get('binance') if isinstance(value, dict) else np.nan).to_frame('factor_1')
    factor_1['factor_1'] = pd.to_numeric(factor_1['factor_1'], errors='coerce')
    factor_2 = read_required_parquet(file_path2, 'v').rename(
        columns={'v': 'factor_2'})
    factor_2['factor_2'] = pd.to_numeric(factor_2['factor_2'], errors='coerce')

    df = pd.merge(factor_1, factor_2, how='inner', left_index=True, right_index=True)
    df = df.sort_index()
    df['data'] = df['factor_1'] * df['factor_2']
    df = df[['factor_1', 'factor_2', 'data']]
    df = process_data(
        df,
        transform,
        model,
        logic,
        side,
        window,
        threshold_1,
        threshold_2,
        asset,
        price_shift_candle_minute=price_shift_candle_minute,
    )
    df.index.name = 'time'
    append_df_to_csv(df, f"{alpha_id}.csv")
    return df




def process_data(
        df,
        transform,
        model,
        logic,
        side,
        window,
        threshold_1,
        threshold_2,
        asset,
        price_shift_candle_minute=None):
    df = slice_alpha_model_input(df)

    df['data'] = df['data'].replace([np.inf, -np.inf], np.nan)
    data_min = df['data'].min(skipna=True)
    if pd.notna(data_min):
        df['data'] = (
            df['data'].round(10)
            if abs(data_min) < 1.0
            else df['data'].round(10)
        )

    if transform != 'None':
        df['factor'] = transformation_lib.apply_transformations(df['data'], transform)
    else:
        df['factor'] = df['data']

    df = df.dropna()

    df = models_lib.choose_model(df, window, model)
    df = df.dropna()
    df = entry_exit_logic_lib.signal_logic_2(df, threshold_1, threshold_2, logic, side)
    df = df.dropna()

    if price_shift_candle_minute is None:
        price_shift_candle_minute = config_data.PRICE_SHIFT_CANDLE_MINUTE
    price_shift_candle_minute = int(price_shift_candle_minute)

    if asset == "BTC":
        price_filename = 'bybit-linear_1m_BTCUSDT.csv'
    elif asset == "ETH":
        price_filename = 'bybit-linear_1m_ETHUSDT.csv'
    else:
        raise ValueError(f"Unsupported asset for price merge: {asset}")

    df_price = pd.read_csv(os.path.join(PRICE_FILEPATH, price_filename), index_col='datetime',
                           usecols=['datetime', 'close'])
    df_price.index = pd.to_datetime(df_price.index, utc=True)
    df_price = df_price.sort_index()[['close']].astype(float)
    if price_shift_candle_minute != 0:
        df_price['close'] = df_price['close'].shift(price_shift_candle_minute)
    df_price = df_price.resample(config_data.INTERVAL).last().dropna(subset=['close'])

    if not df.empty:
        start_date = df.index.min()
        end_date = df.index.max()
        complete_index = pd.date_range(start=start_date, end=end_date, freq=config_data.INTERVAL)
        df = df.reindex(complete_index)
        df = df.ffill()

    # 确保时间格式一致
    df.index = pd.to_datetime(df.index, utc=True)
    df_price.index = pd.to_datetime(df_price.index, utc=True)

    # 然后进行合并
    df = df.merge(df_price, left_index=True, right_index=True, how='left', suffixes=('', '_price'))
    df['price_shift_candle_minute'] = price_shift_candle_minute

    return df

# def append_df_to_csv(df, filename):
#     # pass
#     base_path = ALPHA_SIGNAL_PATH
#     # 构建完整文件路径
#     csv_file_path = os.path.join(base_path, filename)
    
#     # 确保目录存在
#     os.makedirs(base_path, exist_ok=True)
    
#     # 直接覆盖写入整个DataFrame，不进行任何追加操作
#     df.to_csv(csv_file_path, mode='w', index=True)

def alpha_id_from_filename_prefix(filename_prefix):
    base_name = os.path.basename(filename_prefix)
    return base_name.split(".")[0]


def atomic_write_csv(df, file_path, index=False):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    tmp_file_path = f"{file_path}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    try:
        df.to_csv(tmp_file_path, index=index)
        delays = (0.25, 0.5, 1.0, 2.0)
        for attempt in range(len(delays) + 1):
            try:
                os.replace(tmp_file_path, file_path)
                return
            except PermissionError:
                if attempt >= len(delays):
                    raise
                time.sleep(delays[attempt])
    finally:
        if os.path.exists(tmp_file_path):
            try:
                os.remove(tmp_file_path)
            except OSError:
                pass


def update_latest_signal_state(alpha_id, snapshot_df, bucket_time, source_file):
    if snapshot_df.empty:
        return

    latest_time = snapshot_df.index.max()
    latest_row = snapshot_df.loc[latest_time]
    if isinstance(latest_row, pd.DataFrame):
        latest_row = latest_row.iloc[-1]
    state_row = {
        "alpha_id": alpha_id,
        "generated_at": pd.Timestamp.now(tz="UTC"),
        "bucket_time": bucket_time,
        "latest_alpha_time": latest_time,
        "factor": latest_row.get("factor"),
        "signal": latest_row.get("signal"),
        "pos": latest_row.get("pos"),
        "close": latest_row.get("close"),
        "price_shift_candle_minute": latest_row.get("price_shift_candle_minute"),
        "source_file": source_file,
    }

    if os.path.exists(LATEST_SIGNAL_STATE_FILE):
        try:
            state_df = pd.read_csv(LATEST_SIGNAL_STATE_FILE)
            state_df = state_df[state_df["alpha_id"] != alpha_id]
        except Exception:
            state_df = pd.DataFrame()
    else:
        state_df = pd.DataFrame()

    state_df = pd.concat([state_df, pd.DataFrame([state_row])], ignore_index=True)
    state_df = state_df.sort_values("alpha_id")
    atomic_write_csv(state_df, LATEST_SIGNAL_STATE_FILE, index=False)


def append_df_to_csv(df, filename_prefix):

    base_path = ALPHA_SIGNAL_PATH
    os.makedirs(base_path, exist_ok=True)

    df = df.copy()
    df.index = pd.to_datetime(df.index, utc=True)

    # Use wall-clock 6-hour bucket for stable monitoring snapshots.
    bucket_time = pd.Timestamp.now(tz="UTC").floor("6H")


    # CSV filename with timestamp
    time_label = bucket_time.strftime("%Y%m%d_%H%M")
    csv_filename = f"{filename_prefix}_{time_label}.csv"
    csv_file_path = os.path.join(base_path, csv_filename)

    output_df = slice_alpha_output(df)
    snapshot_df = output_df[output_df.index <= bucket_time]

    # Overwrite → guarantees ONE file per minute
    atomic_write_csv(snapshot_df, csv_file_path, index=True)
    update_latest_signal_state(
        alpha_id_from_filename_prefix(filename_prefix),
        output_df,
        bucket_time,
        csv_file_path,
    )

    retention_days = 14
    all_files = [
        f for f in os.listdir(base_path)
        if f.startswith(filename_prefix) and f.endswith(".csv")
    ]

    # Parse timestamp from filename
    file_times = []
    for f in all_files:
        try:
            ts_str = f.replace(filename_prefix + "_", "").replace(".csv", "")
            ts = pd.to_datetime(ts_str, format="%Y%m%d_%H%M", utc=True)
            file_times.append((ts, f))
        except Exception:
            continue  # skip files that don't match the pattern

    # Sort by timestamp ascending (oldest first)
    file_times.sort(key=lambda x: x[0])

    # Delete files older than retention_days.
    cutoff_time = bucket_time - pd.Timedelta(days=retention_days)
    for ts, f in file_times:
        if ts < cutoff_time:
            os.remove(os.path.join(base_path, f))


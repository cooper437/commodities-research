import enum
import os
import pandas as pd
from pandas.core.frame import DataFrame
from tqdm import trange
from datetime import datetime

CONTRACTS_PREFIX_MATCHER = 'LE'  # Optional limit if desired
CURRENT_DIR = os.path.dirname(__file__)
RAW_DATA_DIR = os.path.join(
    CURRENT_DIR, '../../data/raw/firstratedata_futures')
PROCESSED_DATA_DIR = os.path.join(
    CURRENT_DIR, '../../data/processed/futures_contracts')
UNIQUE_TRADING_DAYS_LE_CONTRACTS = os.path.join(
    PROCESSED_DATA_DIR, 'unique_trading_days_le_contracts.csv'
)


def convert_contract_csv_to_df(filename):
    csv_as_df = pd.read_csv(
        f"{RAW_DATA_DIR}/{filename}",
        parse_dates=['DateTime'], usecols=['DateTime', 'Open', 'High', 'Low', 'Close', 'Volume']
    )
    return csv_as_df


def convert_unique_trading_days_series(filepath):
    csv_as_df = pd.read_csv(filepath, parse_dates=[
                            'DateTime'], usecols=['DateTime'])
    return sorted(csv_as_df['DateTime'].dt.date)


def csv_files_to_analyze(data_dir, filename_prefix_matcher):
    # Get a list of all the csv files to process
    csv_files = []
    for file in os.listdir(RAW_DATA_DIR):
        if file.startswith(CONTRACTS_PREFIX_MATCHER):
            csv_files.append(file)
    csv_files.sort()
    return csv_files


def generate_empty_day_bar(contract_symbol, a_date) -> dict:
    empty_day_bar = {
        'Symbol': contract_symbol,
        'Date': a_date,
        '11:59 Change': None,
        '12:04 Change': None,
        'Last Bar Change': None
    }
    return empty_day_bar


def get_bar_at_time(a_days_bars_df: pd.DataFrame, time_to_get: datetime) -> pd.Series:
    bars_at_time_df = a_days_bars_df[a_days_bars_df['DateTime'] == time_to_get]
    if bars_at_time_df.empty:
        return None
    return bars_at_time_df.iloc[0]


def get_eleven_fifty_nine_datetime(year, month, day):
    return datetime(year, month, day, 11, 59, 0)


def get_twelve_oh_four_datetime(year, month, day):
    return datetime(year, month, day, 12, 4, 0)


def calculate_change_from_prior_day_bar(todays_open_price, prior_day_bar):
    if prior_day_bar is None:
        return None
    price_change = todays_open_price - prior_day_bar['Close']
    return format(price_change, '.3f')


overnight_changes_df = pd.DataFrame(
    columns=['Symbol', 'Date', '11:59 Change', '12:04 Change', 'Last Bar Change'])
csv_files = csv_files_to_analyze(
    data_dir=RAW_DATA_DIR, filename_prefix_matcher=CONTRACTS_PREFIX_MATCHER)
unique_trading_days_all_le_contracts = convert_unique_trading_days_series(
    UNIQUE_TRADING_DAYS_LE_CONTRACTS)
print(f"Analyzing {len(csv_files)} files")
for contract_to_process in trange(len(csv_files[0:1])):
    file = csv_files[contract_to_process]
    contract_symbol = file[0:-4]
    print(f"Analyzing data for {contract_symbol}")
    contract_df = convert_contract_csv_to_df(file)
    unique_dates_for_contract = sorted(
        list(contract_df['DateTime'].dt.date.unique()))
    for count, a_date in enumerate(unique_dates_for_contract):
        if count is 0:  # We are at the first day of trading for this contract so there is no prior day to compare with
            overnight_changes_df = overnight_changes_df.append(generate_empty_day_bar(
                contract_symbol, a_date), ignore_index=True)
            continue
        index_of_todays_trading_day = unique_trading_days_all_le_contracts.index(
            a_date)
        prior_trading_day_date = unique_trading_days_all_le_contracts[
            index_of_todays_trading_day - 1]
        a_days_bars_df = contract_df[contract_df['DateTime'].dt.date == a_date]
        a_days_bars_df = a_days_bars_df.sort_values(
            by=['DateTime'], ascending=True)
        prior_trading_days_bars_df = contract_df[contract_df['DateTime'].dt.date ==
                                                 prior_trading_day_date]
        # This contract was not traded on the prior trading day so there is nothing to compare
        if prior_trading_days_bars_df.empty:
            overnight_changes_df = overnight_changes_df.append(generate_empty_day_bar(
                contract_symbol, a_date), ignore_index=True)
            continue
        prior_trading_day_eleven_fify_nine_datetime = get_eleven_fifty_nine_datetime(
            prior_trading_day_date.year, prior_trading_day_date.month, prior_trading_day_date.day)
        prior_trading_day_twelve_oh_four_datetime = get_twelve_oh_four_datetime(
            prior_trading_day_date.year, prior_trading_day_date.month, prior_trading_day_date.day)
        prior_trading_day_eleven_fifty_nine_bar = get_bar_at_time(
            a_days_bars_df=prior_trading_days_bars_df, time_to_get=prior_trading_day_eleven_fify_nine_datetime)
        prior_trading_day_twelve_oh_four_bar = get_bar_at_time(
            a_days_bars_df=prior_trading_days_bars_df, time_to_get=prior_trading_day_twelve_oh_four_datetime
        )
        prior_trading_day_last_bar = prior_trading_days_bars_df.iloc[-1]
        first_bar_of_day = a_days_bars_df.iloc[0]
        todays_open_price = first_bar_of_day['Open']
        eleven_fifty_nine_price_change = calculate_change_from_prior_day_bar(
            todays_open_price=todays_open_price, prior_day_bar=prior_trading_day_eleven_fifty_nine_bar)
        twelve_oh_four_price_change = calculate_change_from_prior_day_bar(
            todays_open_price=todays_open_price, prior_day_bar=prior_trading_day_twelve_oh_four_bar
        )
        last_bar_price_change = calculate_change_from_prior_day_bar(
            todays_open_price=todays_open_price, prior_day_bar=prior_trading_day_last_bar
        )
        overnight_changes_df = overnight_changes_df.append({
            'Symbol': contract_symbol,
            'Date': a_date,
            '11:59 Change': eleven_fifty_nine_price_change,
            '12:04 Change': twelve_oh_four_price_change,
            'Last Bar Change': last_bar_price_change
        }, ignore_index=True)
display(overnight_changes_df)

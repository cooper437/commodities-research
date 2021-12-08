import enum
import os
import pandas as pd
from pandas.core.frame import DataFrame
from tqdm import trange
from datetime import datetime, timedelta
import sys
import getopt

'''
Analyze the raw contract data.
Build out a new dataset that shows the overnight changes from one trading day to the next
There are two strategies that can be used here leading to different outputs:
OPEN_CLOSE_TRUE_OPEN:
    Open: use the open price IF there is a bar on the open; if no bar, exclude the overnight change calc for ALL close types for that day
    Close: use the close bar IF that close bar exists; if no bar, exclude the overnight change calc for ONLY that close type for that day
OPEN_CLOSE_SLIDING_OPEN:
    Open: use the first available bar during the open timeframe (first 60 min after 8:30 CT); if no bar during open timeframe, exclude the overnight change calc for ALL close types for that day
    Close: use the close bar IF that close bar exists; for 11:59 and 12:04 CT bars, if there is no bar available for those time, use the last bar of that day

To change the strategy of the script you can pass in an argument as follows:
create_overnight_changes_by_contract.py -s true_open
create_overnight_changes_by_contract.py -s sliding_open
'''


class Open_Close_Bar_Strategy_Enum(enum.Enum):
    true_open = '_true_open'
    sliding_open = '_sliding_open'


CONTRACTS_PREFIX_MATCHER = 'LE'  # Optional limit if desired
CURRENT_DIR = os.path.dirname(__file__)
RAW_DATA_DIR = os.path.join(
    CURRENT_DIR, '../../data/raw/firstratedata_futures')
PROCESSED_DATA_DIR = os.path.join(
    CURRENT_DIR, '../../data/processed/futures_contracts')
UNIQUE_TRADING_DAYS_LE_CONTRACTS_FILE_PATH = os.path.join(
    PROCESSED_DATA_DIR, 'unique_trading_days_le_contracts.csv'
)
TARGET_BASE_FILENAME = 'overnight_changes_by_contract'
TARGET_FILE_EXTENSION = '.csv'
DATE_OF_PIT_OPEN_CHANGE = datetime(2015, 7, 2)
WIDTH_TRADING_WINDOW_OPEN_MINUTES = 60


def get_target_filepath(open_close_bar_analysis_strategy: Open_Close_Bar_Strategy_Enum):
    target_filepath = os.path.join(PROCESSED_DATA_DIR, TARGET_BASE_FILENAME +
                                   open_close_bar_analysis_strategy.value + TARGET_FILE_EXTENSION)
    return target_filepath


def contract_open_time(trading_bar_datetime: datetime):
    '''
    Given the date of a trading bar return the time of day of the open for that same date
    This helps account for the change in open time after the pit closed on 7/2/2015
    '''
    if trading_bar_datetime >= DATE_OF_PIT_OPEN_CHANGE:  # Trading bar is after the change to pit open
        return trading_bar_datetime.replace(hour=9, minute=30, second=0, microsecond=0)
    else:  # Trading bar is before the change to pit open
        return trading_bar_datetime.replace(hour=10, minute=5, second=0, microsecond=0)


def get_true_open_bar_for_day(a_date: datetime.date):
    # Convert the date to a datetime
    as_datetime = datetime.combine(a_date, datetime.min.time())
    this_days_true_open_time = contract_open_time(as_datetime)
    this_days_true_open_bar = a_days_bars_df[a_days_bars_df['DateTime']
                                             == this_days_true_open_time]
    if this_days_true_open_bar.empty:  # The true open bar is missing for this date
        return None
    return this_days_true_open_bar.iloc[0]


def get_df_rows_within_time_window(a_df: pd.DataFrame, window_start_time: datetime, window_end_time: datetime):
    rows_inside_window = a_df.loc[(a_df['DateTime'] >=
                                   window_start_time) & (a_df['DateTime'] <= window_end_time)]
    return rows_inside_window.sort_values(
        by=['DateTime'], ascending=True)


def get_sliding_open_bar_for_day(a_date: datetime.date, a_days_bars_df: pd.DataFrame):
    '''
    Get the first bar of the day that is still within the open window as though its the open bar
    If there are no bars inside the open window return None
    '''
    as_datetime = datetime.combine(a_date, datetime.min.time())
    this_days_true_open_time = contract_open_time(as_datetime)
    end_of_this_days_open_window = this_days_true_open_time + \
        timedelta(minutes=WIDTH_TRADING_WINDOW_OPEN_MINUTES)
    days_within_open_window_df = get_df_rows_within_time_window(
        a_df=a_days_bars_df, window_start_time=this_days_true_open_time, window_end_time=end_of_this_days_open_window)
    if days_within_open_window_df.empty:
        return None
    first_bar_of_day_within_open_window = days_within_open_window_df.iloc[0]
    return first_bar_of_day_within_open_window


def convert_contract_csv_to_df(filename):
    '''
    Convert the raw contract csv data into a dataframe
    '''
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
        '12:59 Change': None,
        '13:04 Change': None,
        'Last Bar Change': None
    }
    return empty_day_bar


def get_bar_at_time(a_days_bars_df: pd.DataFrame, time_to_get: datetime) -> pd.Series:
    bars_at_time_df = a_days_bars_df[a_days_bars_df['DateTime'] == time_to_get]
    if bars_at_time_df.empty:
        return None
    return bars_at_time_df.iloc[0]


def get_twelve_fifty_nine_datetime(year, month, day):
    return datetime(year, month, day, 12, 59, 0)


def get_thirteen_oh_four_datetime(year, month, day):
    return datetime(year, month, day, 13, 4, 0)


def calculate_change_from_prior_day_bar(todays_open_price, prior_day_bar):
    if prior_day_bar is None:
        return None
    price_change = todays_open_price - prior_day_bar['Close']
    return format(price_change, '.3f')


# Default to a sliding open strategy
open_close_bar_analysis_strategy = Open_Close_Bar_Strategy_Enum.sliding_open

# Parse our arguments from the command line to determine the strategy
try:
    opts, args = getopt.getopt(sys.argv[1:], ':s:')
except getopt.GetoptError:
    print('Run this script as follows: create_overnight_changes_by_contract.py -s <bar_analysis_strategy>')
    sys.exit(2)
for opt, arg in opts:
    if opt in ('-s'):
        open_close_bar_analysis_strategy = Open_Close_Bar_Strategy_Enum[arg]


overnight_changes_df = pd.DataFrame(
    columns=['Symbol', 'Date', '12:59 Change', '13:04 Change', 'Last Bar Change'])
csv_files = csv_files_to_analyze(
    data_dir=RAW_DATA_DIR, filename_prefix_matcher=CONTRACTS_PREFIX_MATCHER)
unique_trading_days_all_le_contracts = convert_unique_trading_days_series(
    UNIQUE_TRADING_DAYS_LE_CONTRACTS_FILE_PATH)
print(
    f"Analyzing {len(csv_files)} files using a {open_close_bar_analysis_strategy.name} strategy")
for contract_to_process in trange(len(csv_files)):
    file = csv_files[contract_to_process]
    contract_symbol = file[0:-4]
    print(f"Analyzing data for {contract_symbol}")
    contract_df = convert_contract_csv_to_df(file)
    unique_dates_for_contract = sorted(
        list(contract_df['DateTime'].dt.date.unique()))
    for count, a_date in enumerate(unique_dates_for_contract):
        if count == 0:  # We are at the first day of trading for this contract so there is no prior day to compare with
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
        prior_trading_day_twelve_fify_nine_datetime = get_twelve_fifty_nine_datetime(
            prior_trading_day_date.year, prior_trading_day_date.month, prior_trading_day_date.day)
        prior_trading_day_thirteen_oh_four_datetime = get_thirteen_oh_four_datetime(
            prior_trading_day_date.year, prior_trading_day_date.month, prior_trading_day_date.day)
        prior_trading_day_twelve_fifty_nine_bar = get_bar_at_time(
            a_days_bars_df=prior_trading_days_bars_df, time_to_get=prior_trading_day_twelve_fify_nine_datetime)
        prior_trading_day_thirteen_oh_four_bar = get_bar_at_time(
            a_days_bars_df=prior_trading_days_bars_df, time_to_get=prior_trading_day_thirteen_oh_four_datetime
        )
        prior_trading_day_last_bar = prior_trading_days_bars_df.iloc[-1]
        this_days_open_bar = get_true_open_bar_for_day(a_date=a_date)
        if this_days_open_bar is None:  # The true open bar is missing for this day
            if open_close_bar_analysis_strategy.name == Open_Close_Bar_Strategy_Enum.true_open.name:  # We are using a true open strategy
                overnight_changes_df = overnight_changes_df.append(generate_empty_day_bar(
                    contract_symbol, a_date), ignore_index=True)
                continue
            # We are using a sliding open strategy
            elif open_close_bar_analysis_strategy.name == Open_Close_Bar_Strategy_Enum.sliding_open.name:
                this_days_open_bar = get_sliding_open_bar_for_day(
                    a_date=a_date, a_days_bars_df=a_days_bars_df)
                if this_days_open_bar is None:  # There are no bars inside the open window for this day to use
                    overnight_changes_df = overnight_changes_df.append(generate_empty_day_bar(
                        contract_symbol, a_date), ignore_index=True)
                    continue
        todays_open_price = this_days_open_bar['Open']
        twelve_fifty_nine_price_change = calculate_change_from_prior_day_bar(
            todays_open_price=todays_open_price, prior_day_bar=prior_trading_day_twelve_fifty_nine_bar)
        thirteen_oh_four_price_change = calculate_change_from_prior_day_bar(
            todays_open_price=todays_open_price, prior_day_bar=prior_trading_day_thirteen_oh_four_bar
        )
        last_bar_price_change = calculate_change_from_prior_day_bar(
            todays_open_price=todays_open_price, prior_day_bar=prior_trading_day_last_bar
        )
        overnight_changes_df = overnight_changes_df.append({
            'Symbol': contract_symbol,
            'Date': a_date,
            '12:59 Change': twelve_fifty_nine_price_change,
            '13:04 Change': thirteen_oh_four_price_change,
            'Last Bar Change': last_bar_price_change
        }, ignore_index=True)
target_file_dest = get_target_filepath(
    open_close_bar_analysis_strategy=open_close_bar_analysis_strategy)
overnight_changes_df.to_csv(target_file_dest, index=False)

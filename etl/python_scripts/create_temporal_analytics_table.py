import pandas as pd
import os
import time
import enum

CURRENT_DIR = os.path.dirname(__file__)
PROCESSED_DATA_DIR = os.path.join(
    CURRENT_DIR, '../../data/processed/futures_contracts/')
CONTRACT_INTRADAY_SLIDING_OPEN_FILE_PATH = os.path.join(
    CURRENT_DIR, '../../data/processed/futures_contracts/contract_open_enriched_sliding_open.csv')
CONTRACT_INTRADAY_TRUE_OPEN_FILE_PATH = os.path.join(
    CURRENT_DIR, '../../data/processed/futures_contracts/contract_open_enriched_true_open.csv')
TARGET_FILENAME = 'nasdaq_cot_intraday_open_signals_correlation.csv'
TARGET_FILE_DEST = os.path.join(PROCESSED_DATA_DIR, TARGET_FILENAME)

# These parameters allow us to filter out trading activity on days where the contract DTE tends to have missing open bars
FILTER_OUT_DTE_WITH_FREQUENTLY_MISSING_OPEN = True
DTE_FILTER_UPPER_BOUNDARY = 140
DTE_FILTER_LOWER_BOUNDARY = 25

# A minute of particular interest that we calculate some additional statistics
#  on like std deviation, and pct values above median
KEY_OPEN_MINUTE_OF_INTEREST = 60


class ReportTimeInterval(enum.Enum):
    day_of_week = 1
    month = 2
    year = 3


def intraday_open_csv_to_df(filename) -> pd.DataFrame:
    csv_as_df = pd.read_csv(
        filename,
        parse_dates=['DateTime'],
        usecols=[
            'Symbol', 'DateTime', 'Open Minutes Offset', 'Open', 'High', 'Low', 'Close',
            'Volume', 'Price Change From Intraday Open', 'Expiration Date', 'DTE'
        ]
    )
    return csv_as_df


def initialize_target_table_df(report_time_interval: ReportTimeInterval) -> pd.DataFrame:
    '''A dataframe that contains the structure needed for the analytics table that is output by the script'''
    time_interval_column_label = report_time_interval.name
    columns = [time_interval_column_label, 'Open Type', 'ACFO t+30', 'ACFO t+60', f"Std Deviation of Intraday Price Change at Open t+{KEY_OPEN_MINUTE_OF_INTEREST}", 'Max ACFO',
               'Min ACFO', 'Minute of Max ACFO', 'Minute of Min ACFO', 'Median Intraday CFO Value t+60', 'Percent GTE Median CFO t+60']
    initialized_df = pd.DataFrame(columns=columns)
    return initialized_df


def filter_bars_for_dte_with_frequently_missing_open(
    intraday_open_df: pd.DataFrame,
    dte_filter_lower_boundary: int,
    dte_filter_upper_boundary: int
) -> pd.DataFrame:
    '''Filter out days associated with a DTE that is often missing a true open bar'''
    filtered_df = intraday_open_df[(intraday_open_df['DTE'] >= dte_filter_lower_boundary) & (
        intraday_open_df['DTE'] <= dte_filter_upper_boundary)]
    return filtered_df


# Script execution Starts Here
target_file_exists = os.path.exists(TARGET_FILE_DEST)
if target_file_exists:
    print('The target file already exists and will be overwritten. Abort in the next 5 seconds to cancel.')
    time.sleep(5)

print("Loading the intraday sliding open dataframe into memory")
intraday_sliding_open_df = intraday_open_csv_to_df(
    CONTRACT_INTRADAY_SLIDING_OPEN_FILE_PATH)
print(
    f"Filtering SLIDING OPEN dataframe to exclude rows where DTE is not between {DTE_FILTER_LOWER_BOUNDARY} and {DTE_FILTER_UPPER_BOUNDARY}")
intraday_sliding_open_df = filter_bars_for_dte_with_frequently_missing_open(
    intraday_open_df=intraday_sliding_open_df,
    dte_filter_lower_boundary=DTE_FILTER_LOWER_BOUNDARY,
    dte_filter_upper_boundary=DTE_FILTER_UPPER_BOUNDARY
)
print("Loading the intraday true open dataframe into memory")
intraday_true_open_df = intraday_open_csv_to_df(
    CONTRACT_INTRADAY_TRUE_OPEN_FILE_PATH)
print(
    f"Filtering TRUE OPEN dataframe to exclude rows where DTE is not between {DTE_FILTER_LOWER_BOUNDARY} and {DTE_FILTER_UPPER_BOUNDARY}")
intraday_true_open_df = filter_bars_for_dte_with_frequently_missing_open(
    intraday_open_df=intraday_true_open_df,
    dte_filter_lower_boundary=DTE_FILTER_LOWER_BOUNDARY,
    dte_filter_upper_boundary=DTE_FILTER_UPPER_BOUNDARY
)
day_of_week_target_df = initialize_target_table_df(
    ReportTimeInterval.day_of_week)
print(day_of_week_target_df)

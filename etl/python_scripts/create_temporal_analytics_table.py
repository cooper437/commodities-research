from typing import NamedTuple, List
from cytoolz import valmap
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


def calculate_average_intraday_price_change_grouped_by_open_minutes_offset(
    intraday_minute_bars:  NamedTuple
) -> pd.DataFrame:
    '''
    Group the intraday minute bars by their Open Minutes Offset and calculate the mean for each minute. Return all that as a single dataframe
    '''
    intraday_price_change_by_minute_after_open = intraday_minute_bars.groupby(
        'Open Minutes Offset', as_index=False)['Price Change From Intraday Open'].mean()
    to_return_df = pd.DataFrame({
        'Open Minutes Offset': intraday_price_change_by_minute_after_open['Open Minutes Offset'],
        'Mean Intraday Price Change': intraday_price_change_by_minute_after_open['Price Change From Intraday Open']
    })
    return to_return_df


def group_df_by_day_of_week(intraday_minute_bars: pd.DataFrame) -> dict:
    '''
    Group the intraday minute bars by day of the week. Return a dict where each key is the number of the day of the week and
    the value is a dataframe containing the rows of intraday_minute_bars corresponding to that particular day of the week
    '''
    days_of_week = [*range(0, 7, 1)]
    intraday_dfs_grouped_by_day_of_week = {}
    for a_day in days_of_week:
        a_single_days_df = intraday_minute_bars[intraday_minute_bars['DateTime'].dt.dayofweek == a_day]
        intraday_dfs_grouped_by_day_of_week[a_day] = a_single_days_df\
            .copy().reset_index()
    return intraday_dfs_grouped_by_day_of_week


def group_df_by_month_of_year(intraday_minute_bars: pd.DataFrame) -> dict:
    '''
    Group the intraday minute bars by month of the year. Return a dict where each key is the number of the month and
    the value is a dataframe containing the rows of intraday_minute_bars corresponding to that particular month
    '''
    months_of_year = [*range(1, 13, 1)]
    intraday_dfs_grouped_by_month = {}
    for a_month in months_of_year:
        a_single_months_df = intraday_minute_bars[intraday_minute_bars['DateTime'].dt.month == a_month]
        intraday_dfs_grouped_by_month[a_month] = a_single_months_df\
            .copy().reset_index()
    return intraday_dfs_grouped_by_month


def group_df_by_year(intraday_minute_bars: pd.DataFrame) -> dict:
    '''
    Group the intraday minute bars by year. Return a dict where each key is the year and
    the value is a dataframe containing the rows of intraday_minute_bars corresponding to that particular year
    '''
    distinct_years = intraday_minute_bars['DateTime'].dt.year\
        .drop_duplicates().to_list()
    intraday_dfs_grouped_by_year = {}
    for a_year in distinct_years:
        a_single_years_df = intraday_minute_bars[intraday_minute_bars['DateTime'].dt.year == a_year]
        intraday_dfs_grouped_by_year[a_year] = a_single_years_df\
            .copy().reset_index()
    return intraday_dfs_grouped_by_year


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
# Initialize our output dataframes one for each time interval
day_of_week_target_df = initialize_target_table_df(
    ReportTimeInterval.day_of_week)
month_of_year_target_df = initialize_target_table_df(
    ReportTimeInterval.month)
year_target_df = initialize_target_table_df(
    ReportTimeInterval.year)
# Split our dataframes apart in grouping the day, month, and year respectively
intraday_true_open_grouped_by_day_of_week = group_df_by_day_of_week(
    intraday_true_open_df)
intraday_true_open_grouped_by_month = group_df_by_month_of_year(
    intraday_true_open_df)
intraday_true_open_grouped_by_year = group_df_by_year(intraday_true_open_df)
# Use our split dataframes to generate a new dataframe showing the average intraday price change at each minute after the open
avg_changes_by_minute_true_open_grouped_by_day_of_week = valmap(
    calculate_average_intraday_price_change_grouped_by_open_minutes_offset,
    intraday_true_open_grouped_by_day_of_week
)
print(intraday_true_open_grouped_by_day_of_week)

import enum
import time
import os
import numpy as np
from decimal import Decimal, ROUND_HALF_UP
import pandas as pd
from cytoolz import valmap, itemmap, keymap
from typing import NamedTuple, Tuple
import logging

logging.basicConfig(
    format='%(asctime)s - %(levelname)s:%(message)s', level=logging.INFO)

CURRENT_DIR = os.path.dirname(__file__)
PROCESSED_DATA_DIR = os.path.join(
    CURRENT_DIR, '../../data/processed/futures_contracts/')
CONTRACT_INTRADAY_SLIDING_OPEN_FILE_PATH = os.path.join(
    CURRENT_DIR, '../../data/processed/futures_contracts/contract_open_enriched_sliding_open.csv')
CONTRACT_INTRADAY_TRUE_OPEN_FILE_PATH = os.path.join(
    CURRENT_DIR, '../../data/processed/futures_contracts/contract_open_enriched_true_open.csv')
SETLLEMENT_CHANGE_DATA_PATH = os.path.join(
    CURRENT_DIR, '../../data/processed/futures_contracts/settlement_analytics'
)
SETLLEMENT_CHANGE_DATA_BASE_FILENAME = 'changes_from_settlement_'

# These parameters allow us to filter out trading activity on days where the contract DTE tends to have missing open bars
DTE_FILTER_UPPER_BOUNDARY = 140
DTE_FILTER_LOWER_BOUNDARY = 25


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


def settlement_data_changes_median(interval: str, filename: str) -> pd.DataFrame:
    '''
    Get the median value for a particular open type and interval using the previously created temporal analytics tables
    '''
    csv_as_df = pd.read_csv(
        filename,
        parse_dates=['Date'],
        usecols=['Date', 'Symbol',
                 'Price Difference b/w Open And Prior Day Settlement', 'Days Looking Back']
    )
    populated_price_only_df = csv_as_df[~csv_as_df['Price Difference b/w Open And Prior Day Settlement'].isnull()]
    price_diff_series = populated_price_only_df['Price Difference b/w Open And Prior Day Settlement'].sort_values(
        ignore_index=True)
    median = price_diff_series.median()
    return (interval, median)


def construct_full_path(filename_with_open_type: str, interval: str):
    full_path = f"{filename_with_open_type}_{interval}.csv"
    return full_path


def calc_median_for_intervals_of_open_type(intervals_for_open_type: Tuple, filename: str):
    (open_type, intervals) = intervals_for_open_type
    filename_with_open_type = filename + open_type
    interval_data_frames = itemmap(
        lambda interval: settlement_data_changes_median(interval=interval[0], filename=construct_full_path(filename_with_open_type, interval[0])), intervals)
    return (open_type, interval_data_frames)


def get_median_settlement_data_values(settlement_changes_base_filename: str, settlement_changes_base_file_path: str) -> dict:
    '''
    Gather the median settlement data values for every combination of open type and interval
    '''
    median_price_vals = {
        'true_open': {
            'overnight': None,
            'weekly': None,
            'monthly': None,
            'annualy': None
        },
        'sliding_open': {
            'overnight': None,
            'weekly': None,
            'monthly': None,
            'annualy': None
        }
    }
    median_price_vals = itemmap(lambda item: calc_median_for_intervals_of_open_type(
        item, f"{settlement_changes_base_file_path}/{settlement_changes_base_filename}"), median_price_vals)
    return median_price_vals


def round_to_nearest_thousandth(np_float: np.float64) -> Decimal:
    rounded = Decimal(np_float).quantize(
        Decimal('0.001'), rounding=ROUND_HALF_UP)
    return rounded


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
    intraday_minute_bars_df:  NamedTuple
) -> pd.DataFrame:
    '''
    Group the intraday minute bars by their Open Minutes Offset and calculate the mean for each minute. Return all that as a single dataframe
    '''
    # First filter out all rows where Open Minutes Offset equals 60 because these are really the 61st minute after open which is outside the open window
    intraday_minute_bars_df_filtered = intraday_minute_bars_df[
        intraday_minute_bars_df['Open Minutes Offset'] != 60]
    intraday_price_change_by_minute_after_open = intraday_minute_bars_df_filtered.groupby(
        'Open Minutes Offset', as_index=False)['Price Change From Intraday Open'].mean()
    to_return_df = pd.DataFrame({
        'Open Minutes Offset': intraday_price_change_by_minute_after_open['Open Minutes Offset'],
        'Mean Intraday Price Change': intraday_price_change_by_minute_after_open['Price Change From Intraday Open']
    })
    return to_return_df


def split_intraday_minute_bars_by_median_df(intraday_minute_bars_df:  pd.DataFrame, median_val_to_split_on):
    above_median_df = intraday_minute_bars_df[intraday_minute_bars_df[
        'Price Change From Intraday Open'] >= median_val_to_split_on]
    below_median_df = intraday_minute_bars_df[intraday_minute_bars_df[
        'Price Change From Intraday Open'] < median_val_to_split_on]
    return {
        'Value Splitting Data': median_val_to_split_on,
        'above_median_df': above_median_df,
        'below_median_df': below_median_df
    }


logging.info("Loading the intraday sliding open dataframe into memory")
intraday_sliding_open_df = intraday_open_csv_to_df(
    CONTRACT_INTRADAY_SLIDING_OPEN_FILE_PATH)
logging.info("Loading the intraday true open dataframe into memory")
intraday_true_open_df = intraday_open_csv_to_df(
    CONTRACT_INTRADAY_TRUE_OPEN_FILE_PATH)
logging.info(
    f"Filtering SLIDING OPEN dataframe to exclude rows where DTE is not between {DTE_FILTER_LOWER_BOUNDARY} and {DTE_FILTER_UPPER_BOUNDARY}")
intraday_sliding_open_df = filter_bars_for_dte_with_frequently_missing_open(
    intraday_open_df=intraday_sliding_open_df,
    dte_filter_lower_boundary=DTE_FILTER_LOWER_BOUNDARY,
    dte_filter_upper_boundary=DTE_FILTER_UPPER_BOUNDARY
)
logging.info(
    f"Filtering TRUE OPEN dataframe to exclude rows where DTE is not between {DTE_FILTER_LOWER_BOUNDARY} and {DTE_FILTER_UPPER_BOUNDARY}")
intraday_true_open_df = filter_bars_for_dte_with_frequently_missing_open(
    intraday_open_df=intraday_true_open_df,
    dte_filter_lower_boundary=DTE_FILTER_LOWER_BOUNDARY,
    dte_filter_upper_boundary=DTE_FILTER_UPPER_BOUNDARY
)

median_settlement_values = get_median_settlement_data_values(
    settlement_changes_base_filename=SETLLEMENT_CHANGE_DATA_BASE_FILENAME, settlement_changes_base_file_path=SETLLEMENT_CHANGE_DATA_PATH)
open_split_data = {
    'true_open': valmap(
        lambda median_settlement_value: split_intraday_minute_bars_by_median_df(
            intraday_minute_bars_df=intraday_true_open_df,
            median_val_to_split_on=median_settlement_value
        ),
        median_settlement_values['true_open']
    ),
    'sliding_open': valmap(
        lambda median_settlement_value: split_intraday_minute_bars_by_median_df(
            intraday_minute_bars_df=intraday_sliding_open_df,
            median_val_to_split_on=median_settlement_value
        ),
        median_settlement_values['sliding_open']
    )
}
# true_open_split_by_settlement = valmap(
#     lambda median_settlement_value: split_intraday_minute_bars_by_median_df(
#         intraday_minute_bars_df=intraday_true_open_df,
#         median_val_to_split_on=median_settlement_value
#     ),
#     median_settlement_values['true_open']
# )
# sliding_open_split_by_settlement = valmap(
#     lambda median_settlement_value: split_intraday_minute_bars_by_median_df(
#         intraday_minute_bars_df=intraday_sliding_open_df,
#         median_val_to_split_on=median_settlement_value
#     ),
#     median_settlement_values['sliding_open']
# )
print('hello')
# valmap(
#     lambda val: calculate_average_intraday_price_change_grouped_by_open_minutes_offset(
#         val),
#     median_settlement_values['true_open']
# )

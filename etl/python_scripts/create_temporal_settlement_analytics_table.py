import enum
import time
import os
import numpy as np
from decimal import Decimal, ROUND_HALF_UP
import pandas as pd
from tqdm import trange
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


def settlement_data_changes_median(interval: str, filename: str) -> Tuple[float, dict]:
    '''
    Get the median value for a particular open type and interval using the previously created overnight settlement datasets.
    Return a tuple where the first element is the interval type we are dealing with (overnight, weekly, monthly, yearly) and the second element
    is a dict containing the median value to split upon and the dates above and below that median by contract
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
    unique_symbols = populated_price_only_df['Symbol'].unique().tolist()
    dates_above_below_by_contract = {}
    for symbol in unique_symbols:
        for_symbol_df = csv_as_df[csv_as_df['Symbol'] == symbol]
        ge_median_dates = for_symbol_df[for_symbol_df[
            'Price Difference b/w Open And Prior Day Settlement'] >= median]['Date'].dt.date.tolist()
        lt_median_dates = for_symbol_df[for_symbol_df[
            'Price Difference b/w Open And Prior Day Settlement'] < median]['Date'].dt.date.tolist()
        dates_above_below_by_contract[symbol] = {
            'ge_median_dates': ge_median_dates,
            'lt_median_dates': lt_median_dates
        }
    return (interval, {
        'Value Splitting Data': median,
        'dates_above_below_by_contract': dates_above_below_by_contract
    })


def construct_full_path(filename_with_open_type: str, interval: str):
    full_path = f"{filename_with_open_type}_{interval}.csv"
    return full_path


def calc_median_for_intervals_of_open_type(intervals_for_open_type: Tuple, filename: str):
    (open_type, intervals) = intervals_for_open_type
    filename_with_open_type = filename + open_type
    interval_data_frames = itemmap(
        lambda interval: settlement_data_changes_median(interval=interval[0], filename=construct_full_path(filename_with_open_type, interval[0])), intervals)
    return (open_type, interval_data_frames)


def get_median_settlement_dates_and_values(settlement_changes_base_filename: str, settlement_changes_base_file_path: str) -> dict:
    '''
    Gather the median settlement data values and dates above/below median for every combination of open type and interval
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


def calculate_pct_above_below_intraday_open_price_change_at_open_minute_offset(
    intraday_minute_bars_df:  NamedTuple,
    open_minute_offset: int,
    intraday_price_cfo_at_key_minute_median: np.float64
) -> dict:
    '''
    Calculate the percentage of rows that are above and below the median CFO value at a particular minute after the open
    '''
    intraday_price_change_at_minute_series = intraday_minute_bars_df[intraday_minute_bars_df[
        'Open Minutes Offset'] == open_minute_offset]['Price Change From Intraday Open']
    total_series_length = intraday_price_change_at_minute_series.size
    num_rows_gte_median = intraday_price_change_at_minute_series[
        intraday_price_change_at_minute_series >= intraday_price_cfo_at_key_minute_median].size
    num_rows_lt_median = intraday_price_change_at_minute_series[
        intraday_price_change_at_minute_series < intraday_price_cfo_at_key_minute_median].size
    pct_gte_median = round(
        (num_rows_gte_median / total_series_length) * 100, 4)
    return pct_gte_median


def calculate_minmax_acfo(avg_changes_by_minute_after_open_df:  NamedTuple) -> dict:
    max_minute = avg_changes_by_minute_after_open_df.iloc[
        avg_changes_by_minute_after_open_df['Mean Intraday Price Change'].idxmax()]
    min_minute = avg_changes_by_minute_after_open_df.iloc[
        avg_changes_by_minute_after_open_df['Mean Intraday Price Change'].idxmin()]
    return {
        'Minute of Max ACFO': max_minute['Open Minutes Offset'],
        'Minute of Min ACFO': min_minute['Open Minutes Offset'],
        'Max ACFO': round_to_nearest_thousandth(max_minute['Mean Intraday Price Change']),
        'Min ACFO': round_to_nearest_thousandth(min_minute['Mean Intraday Price Change'])
    }


def gather_temporal_statistics_on_open(
    avg_changes_by_minute_after_open_df: pd.DataFrame,
    intraday_minute_bars_df: pd.DataFrame,
    median_cfo_value_at_t_sixty_for_whole_dataset: np.float64
) -> pd.DataFrame:
    if avg_changes_by_minute_after_open_df.empty:
        return {
            'ACFO t+30': None,
            'ACFO t+60': None,
            'Std Deviation of Intraday Price Change at Open t+60': None,
            'Percent GTE Median CFO t+60': None,
            'Minute of Max ACFO': None,
            'Minute of Min ACFO': None,
            'Max ACFO': None,
            'Min ACFO': None
        }
    acfo_at_thirty_mins = avg_changes_by_minute_after_open_df[avg_changes_by_minute_after_open_df[
        'Open Minutes Offset'] == 29]['Mean Intraday Price Change'].iloc[0]
    acfo_at_sixty_mins = avg_changes_by_minute_after_open_df[avg_changes_by_minute_after_open_df[
        'Open Minutes Offset'] == 59]['Mean Intraday Price Change'].iloc[0]
    std_deviation_at_t_sixty = intraday_minute_bars_df[intraday_minute_bars_df[
        'Open Minutes Offset'] == 59]['Price Change From Intraday Open'].std()
    pct_above_median_at_t_sixty = calculate_pct_above_below_intraday_open_price_change_at_open_minute_offset(
        intraday_minute_bars_df=intraday_minute_bars_df,
        open_minute_offset=59,
        intraday_price_cfo_at_key_minute_median=median_cfo_value_at_t_sixty_for_whole_dataset
    )
    min_max_acfo = calculate_minmax_acfo(avg_changes_by_minute_after_open_df)
    return {
        'ACFO t+30': round_to_nearest_thousandth(acfo_at_thirty_mins),
        'ACFO t+60': round_to_nearest_thousandth(acfo_at_sixty_mins),
        'Std Deviation of Intraday Price Change at Open t+60': round_to_nearest_thousandth(std_deviation_at_t_sixty),
        'Percent GTE Median CFO t+60': round_to_nearest_thousandth(pct_above_median_at_t_sixty),
        **min_max_acfo
    }


def analyze_open_type(
    intraday_minute_bars_df:  pd.DataFrame,
    settlement_median_data: dict,
    median_cfo_value_at_t_sixty_for_whole_dataset: np.float64,
    open_type: str
):
    unique_symbols = [*settlement_median_data['dates_above_below_by_contract'].keys(
    )]
    ge_median_df = pd.DataFrame()
    lt_median_df = pd.DataFrame()
    for symbol_index in trange(len(unique_symbols)):
        symbol = unique_symbols[symbol_index]
        intraday_minute_bars_for_symbol_df = intraday_minute_bars_df[
            intraday_minute_bars_df['Symbol'] == symbol]
        dates_ge_median = settlement_median_data['dates_above_below_by_contract'][symbol]['ge_median_dates']
        dates_lt_median = settlement_median_data['dates_above_below_by_contract'][symbol]['lt_median_dates']
        ge_median_for_symbol_df = intraday_minute_bars_for_symbol_df[intraday_minute_bars_for_symbol_df.DateTime.dt.date.isin(
            dates_ge_median)]
        lt_median_for_symbol_df = intraday_minute_bars_for_symbol_df[intraday_minute_bars_for_symbol_df.DateTime.dt.date.isin(
            dates_lt_median)]
        ge_median_df = pd.concat([ge_median_df, ge_median_for_symbol_df])
        lt_median_df = pd.concat([lt_median_df, lt_median_for_symbol_df])
    avg_intraday_price_change_ge_median_df = calculate_average_intraday_price_change_grouped_by_open_minutes_offset(
        ge_median_df)
    avg_intraday_price_change_lt_median_df = calculate_average_intraday_price_change_grouped_by_open_minutes_offset(
        lt_median_df)
    ge_median_temporal_stats_on_open = gather_temporal_statistics_on_open(
        avg_changes_by_minute_after_open_df=avg_intraday_price_change_ge_median_df,
        intraday_minute_bars_df=ge_median_df,
        median_cfo_value_at_t_sixty_for_whole_dataset=median_cfo_value_at_t_sixty_for_whole_dataset
    )
    lt_median_temporal_stats_on_open = gather_temporal_statistics_on_open(
        avg_changes_by_minute_after_open_df=avg_intraday_price_change_lt_median_df,
        intraday_minute_bars_df=lt_median_df,
        median_cfo_value_at_t_sixty_for_whole_dataset=median_cfo_value_at_t_sixty_for_whole_dataset
    )
    output_df = pd.DataFrame()
    output_df = output_df.append({
        'Value Splitting Data': settlement_median_data['Value Splitting Data'],
        'Above/Below Median': 'above',
        **ge_median_temporal_stats_on_open
    }, ignore_index=True)
    output_df = output_df.append({
        'Value Splitting Data': settlement_median_data['Value Splitting Data'],
        'Above/Below Median': 'below',
        **lt_median_temporal_stats_on_open
    }, ignore_index=True)
    return output_df
    # return {
    #     'Value Splitting Data': settlement_median_data['Value Splitting Data'],
    #     'ge_median_df': ge_median_df,
    #     'lt_median_df': lt_median_df,
    #     'avg_intraday_price_change_ge_median_df': avg_intraday_price_change_ge_median_df,
    #     'avg_intraday_price_change_lt_median_df': avg_intraday_price_change_lt_median_df
    # }


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
median_at_t_sixty_sliding_open = intraday_sliding_open_df[intraday_sliding_open_df[
    'Open Minutes Offset'] == 59]['Price Change From Intraday Open'].sort_values().median()
median_at_t_sixty_true_open = intraday_true_open_df[intraday_true_open_df[
    'Open Minutes Offset'] == 59]['Price Change From Intraday Open'].sort_values().median()

median_settlement_values = get_median_settlement_dates_and_values(
    settlement_changes_base_filename=SETLLEMENT_CHANGE_DATA_BASE_FILENAME, settlement_changes_base_file_path=SETLLEMENT_CHANGE_DATA_PATH)
open_split_data = {
    'true_open': valmap(
        lambda val: analyze_open_type(
            intraday_minute_bars_df=intraday_true_open_df,
            settlement_median_data=val,
            median_cfo_value_at_t_sixty_for_whole_dataset=median_at_t_sixty_true_open,
            open_type='true_open'
        ),
        median_settlement_values['true_open']
    ),
    'sliding_open': valmap(
        lambda val: analyze_open_type(
            intraday_minute_bars_df=intraday_sliding_open_df,
            settlement_median_data=val,
            median_cfo_value_at_t_sixty_for_whole_dataset=median_at_t_sixty_sliding_open,
            open_type='sliding_open'
        ),
        median_settlement_values['sliding_open']
    )
}
print('hello')
# valmap(
#     lambda val: calculate_average_intraday_price_change_grouped_by_open_minutes_offset(
#         val),
#     median_settlement_values['true_open']
# )

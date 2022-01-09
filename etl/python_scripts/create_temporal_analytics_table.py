from typing import NamedTuple, List
from cytoolz import valmap, itemmap, keymap
import pandas as pd
import numpy as np
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
TARGET_FILENAME = '**.csv'
TARGET_FILE_DEST = os.path.join(PROCESSED_DATA_DIR, TARGET_FILENAME)

# These parameters allow us to filter out trading activity on days where the contract DTE tends to have missing open bars
FILTER_OUT_DTE_WITH_FREQUENTLY_MISSING_OPEN = True
DTE_FILTER_UPPER_BOUNDARY = 140
DTE_FILTER_LOWER_BOUNDARY = 25

# A minute of particular interest that we calculate some additional statistics
#  on like std deviation, and pct values above median
KEY_OPEN_MINUTE_OF_INTEREST = 60


class ReportTimeInterval(enum.Enum):
    day_of_week = 'Day Of Week'
    month = 'Month'
    year = 'Year'


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
    time_interval_column_label = report_time_interval.value
    # columns = [time_interval_column_label, 'Open Type', 'ACFO t+30', 'ACFO t+60', f"Std Deviation of Intraday Price Change at Open t+{KEY_OPEN_MINUTE_OF_INTEREST}", 'Max ACFO',
    #            'Min ACFO', 'Minute of Max ACFO', 'Minute of Min ACFO', 'Median Intraday CFO Value t+60', 'Percent GTE Median CFO t+60']
    columns = [time_interval_column_label, 'Open Type']
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


def split_df_by_day_of_week(intraday_minute_bars_df: pd.DataFrame) -> dict:
    '''
    Split the intraday minute bars by day of the week. Return a dict where each key is the number of the day of the week and
    the value is a dataframe containing the rows of intraday_minute_bars corresponding to that particular day of the week
    '''
    days_of_week = {0: 'Monday', 1: 'Tuesday', 2: 'Wednesday',
                    3: 'Thursday', 4: 'Friday', 5: 'Saturday', 6: 'Sunday'}
    intraday_dfs_grouped_by_day_of_week = {}
    for a_day in list(days_of_week.keys()):
        a_single_days_df = intraday_minute_bars_df[intraday_minute_bars_df['DateTime'].dt.dayofweek == a_day]
        intraday_dfs_grouped_by_day_of_week[days_of_week[a_day]] = a_single_days_df\
            .copy().reset_index()
    return intraday_dfs_grouped_by_day_of_week


def split_df_by_month_of_year(intraday_minute_bars_df: pd.DataFrame) -> dict:
    '''
    Split the intraday minute bars by month of the year. Return a dict where each key is the number of the month and
    the value is a dataframe containing the rows of intraday_minute_bars corresponding to that particular month
    '''
    months_of_year = [*range(1, 13, 1)]
    intraday_dfs_grouped_by_month = {}
    for a_month in months_of_year:
        a_single_months_df = intraday_minute_bars_df[intraday_minute_bars_df['DateTime'].dt.month == a_month]
        intraday_dfs_grouped_by_month[a_month] = a_single_months_df\
            .copy().reset_index()
    return intraday_dfs_grouped_by_month


def split_df_by_year(intraday_minute_bars_df: pd.DataFrame) -> dict:
    '''
    Split the intraday minute bars by year. Return a dict where each key is the year and
    the value is a dataframe containing the rows of intraday_minute_bars corresponding to that particular year
    '''
    distinct_years = intraday_minute_bars_df['DateTime'].dt.year\
        .drop_duplicates().to_list()
    intraday_dfs_grouped_by_year = {}
    for a_year in distinct_years:
        a_single_years_df = intraday_minute_bars_df[intraday_minute_bars_df['DateTime'].dt.year == a_year]
        intraday_dfs_grouped_by_year[a_year] = a_single_years_df\
            .copy().reset_index()
    return intraday_dfs_grouped_by_year


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
        'Max ACFO': max_minute['Mean Intraday Price Change'],
        'Min ACFO': min_minute['Mean Intraday Price Change']
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
        'ACFO t+30': acfo_at_thirty_mins,
        'ACFO t+60': acfo_at_sixty_mins,
        'Std Deviation of Intraday Price Change at Open t+60': std_deviation_at_t_sixty,
        'Percent GTE Median CFO t+60': pct_above_median_at_t_sixty,
        **min_max_acfo
    }


def analyze_open_type(intraday_minute_bars_df: pd.DataFrame) -> pd.DataFrame:
    median_cfo_value_at_t_sixty_for_whole_dataset = intraday_minute_bars_df[intraday_minute_bars_df[
        'Open Minutes Offset'] == 59]['Price Change From Intraday Open'].median()
    # Split our dataframes apart in grouping the day, month, and year respectively
    intraday_split_by_day_of_week = split_df_by_day_of_week(
        intraday_minute_bars_df)
    intraday_split_by_month = split_df_by_month_of_year(
        intraday_minute_bars_df)
    intraday_split_by_year = split_df_by_year(
        intraday_minute_bars_df)
    # Use our split dataframes to generate a new dataframe showing the average intraday price change at each minute after the open
    avg_changes_grouped_by_minute_split_by_day_of_week = valmap(
        calculate_average_intraday_price_change_grouped_by_open_minutes_offset,
        intraday_split_by_day_of_week
    )
    avg_changes_grouped_by_minute_split_by_month_of_year = valmap(
        calculate_average_intraday_price_change_grouped_by_open_minutes_offset,
        intraday_split_by_month
    )
    avg_changes_grouped_by_minute_split_by_year = valmap(
        calculate_average_intraday_price_change_grouped_by_open_minutes_offset,
        intraday_split_by_year
    )
    temporal_open_stats_split_by_day_of_week = {}
    temporal_open_stats_split_by_month = {}
    temporal_open_stats_split_by_year = {}
    for key, value in avg_changes_grouped_by_minute_split_by_day_of_week.items():
        temporal_open_stats_split_by_day_of_week[key] = gather_temporal_statistics_on_open(
            avg_changes_by_minute_after_open_df=value,
            intraday_minute_bars_df=intraday_split_by_day_of_week[key],
            median_cfo_value_at_t_sixty_for_whole_dataset=median_cfo_value_at_t_sixty_for_whole_dataset
        )
    for key, value in avg_changes_grouped_by_minute_split_by_month_of_year.items():
        temporal_open_stats_split_by_month[key] = gather_temporal_statistics_on_open(
            avg_changes_by_minute_after_open_df=value,
            intraday_minute_bars_df=intraday_split_by_month[key],
            median_cfo_value_at_t_sixty_for_whole_dataset=median_cfo_value_at_t_sixty_for_whole_dataset
        )
    for key, value in avg_changes_grouped_by_minute_split_by_year.items():
        temporal_open_stats_split_by_year[key] = gather_temporal_statistics_on_open(
            avg_changes_by_minute_after_open_df=value,
            intraday_minute_bars_df=intraday_split_by_year[key],
            median_cfo_value_at_t_sixty_for_whole_dataset=median_cfo_value_at_t_sixty_for_whole_dataset
        )
    return {
        'by_day_of_week': temporal_open_stats_split_by_day_of_week,
        'by_month': temporal_open_stats_split_by_month,
        'by_year': temporal_open_stats_split_by_year
    }


def merge_daily_stats_into_df(true_open_daily, sliding_open_daily) -> pd.DataFrame:
    day_of_week_target_df = initialize_target_table_df(
        ReportTimeInterval.day_of_week)
    # Append true open dicts to the dataframe
    for key, value in true_open_daily.items():
        day_of_week_target_df = day_of_week_target_df.append(
            {**value, 'Day Of Week': key, 'Open Type': 'true_open'}, ignore_index=True)
    # Append sliding open dicts to the dataframe
    for key, value in sliding_open_daily.items():
        day_of_week_target_df = day_of_week_target_df.append(
            {**value, 'Day Of Week': key, 'Open Type': 'sliding_open'}, ignore_index=True)
    return day_of_week_target_df


def merge_monthly_stats_into_df(true_open_monthly, sliding_open_monthly) -> pd.DataFrame:
    monthly_target_df = initialize_target_table_df(
        ReportTimeInterval.month)
    # Append true open dicts to the dataframe
    for key, value in true_open_monthly.items():
        monthly_target_df = monthly_target_df.append(
            {**value, 'Month': key, 'Open Type': 'true_open'}, ignore_index=True)
    # Append sliding open dicts to the dataframe
    for key, value in sliding_open_monthly.items():
        monthly_target_df = monthly_target_df.append(
            {**value, 'Month': key, 'Open Type': 'sliding_open'}, ignore_index=True)
    return monthly_target_df


def merge_yearly_stats_into_df(true_open_yearly, sliding_open_yearly) -> pd.DataFrame:
    yearly_target_df = initialize_target_table_df(
        ReportTimeInterval.year)
    # Append true open dicts to the dataframe
    for key, value in true_open_yearly.items():
        yearly_target_df = yearly_target_df.append(
            {**value, 'Year': key, 'Open Type': 'true_open'}, ignore_index=True)
    # Append sliding open dicts to the dataframe
    for key, value in sliding_open_yearly.items():
        yearly_target_df = yearly_target_df.append(
            {**value, 'Year': key, 'Open Type': 'sliding_open'}, ignore_index=True)
    return yearly_target_df

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
true_open_stats = analyze_open_type(
    intraday_minute_bars_df=intraday_true_open_df)
sliding_open_stats = analyze_open_type(
    intraday_minute_bars_df=intraday_sliding_open_df
)
day_of_week_target_df = merge_daily_stats_into_df(
    true_open_daily=true_open_stats['by_day_of_week'],
    sliding_open_daily=sliding_open_stats['by_day_of_week']
)
monthly_target_df = merge_monthly_stats_into_df(
    true_open_monthly=true_open_stats['by_month'],
    sliding_open_monthly=sliding_open_stats['by_month']
)
yearly_target_df = merge_monthly_stats_into_df(
    true_open_monthly=true_open_stats['by_year'],
    sliding_open_monthly=sliding_open_stats['by_year']
)
print('hello')

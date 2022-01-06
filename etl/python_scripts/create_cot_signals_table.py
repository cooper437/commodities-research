'''
Generate an analytics table of potential trading signals that correlates every field from the NASDAQ commitment of trade report with
the intraday open datasets we've produced. We show the average change from open at a variety of times after the daily open for each
field in the COT reports. Under the hood what we are doing is as follows for each COT field:
1: Find the median value for the field
2: Find all the dates above and below that median respectively
3: Correlate those dates with actual trading activity at the open
4: Average the price changes on those dates on the minute for every 60 minutes after the open
'''
from typing import List, NamedTuple
from collections import namedtuple
import os
import datetime
import operator
import pandas as pd
from tqdm import trange
import time

CURRENT_DIR = os.path.dirname(__file__)
PROCESSED_DATA_DIR = os.path.join(
    CURRENT_DIR, '../../data/processed/futures_contracts/')
COMMITMENT_OF_TRADERS_REPORTS_BASE_PATH = os.path.join(
    CURRENT_DIR, '../../data/raw/nasdaq_data_link/commitment_of_trade/')
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


def list_reportable_files(base_path: str) -> List[str]:
    # Get a list of all the csv files to process in this script
    csv_files = []
    for file in os.listdir(base_path):
        if file.endswith(".csv"):
            csv_files.append(file)
    csv_files.sort()
    return csv_files


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


def filter_bars_for_dte_with_frequently_missing_open(
    intraday_open_df: pd.DataFrame,
    dte_filter_lower_boundary: int,
    dte_filter_upper_boundary: int
) -> pd.DataFrame:
    '''Filter out days associated with a DTE that is often missing a true open bar'''
    filtered_df = intraday_open_df[(intraday_open_df['DTE'] >= dte_filter_lower_boundary) & (
        intraday_open_df['DTE'] <= dte_filter_upper_boundary)]
    return filtered_df


def initialize_cot_analytics_table_df() -> pd.DataFrame:
    '''A dataframe that contains the structure needed for the analytics table that is output by the script'''
    columns = ['Report Name', 'Field Name', 'Above/Below Median Of CoT Field', 'Open Type', 'Median Value Of CoT Field',
               'ACFO t+30', 'ACFO t+60', f"Std Deviation of Intraday Price Change at Open t+{KEY_OPEN_MINUTE_OF_INTEREST}", 'Max ACFO',
               'Min ACFO', 'Minute of Max ACFO', 'Minute of Min ACFO', 'Median Intraday CFO Value t+60', 'Percent GTE Median CFO t+60']
    initialized_df = pd.DataFrame(columns=columns)
    return initialized_df


def cot_csv_to_df(filename) -> pd.DataFrame:
    '''Convert the commitment of traders report to a dataframe with the relevant columns'''
    csv_as_df = pd.read_csv(
        filename,
        parse_dates=['Date']
    )
    return csv_as_df


def is_nonreportable_column(a_column):
    non_reportable_columns = [
        'Date', '% OF Open Interest (OI) All NoCIT', 'Open Interest - % of OI']
    is_non_reportable_col = operator.not_(any(
        element == a_column for element in non_reportable_columns))
    return is_non_reportable_col


def date_of_preceding_tuesday(a_date: pd.Timestamp) -> pd.Timestamp:
    day_of_week = a_date.date().weekday()
    match day_of_week:
        case 0:  # Monday
            days_since_preceding_tuesday = 6
        case 1:  # Tuesday
            days_since_preceding_tuesday = 7
        case 2:  # Wednesday
            days_since_preceding_tuesday = 8
        case 3:  # Thursday
            days_since_preceding_tuesday = 9
        case 4:  # Friday
            days_since_preceding_tuesday = 10
        case 5:  # Saturday
            days_since_preceding_tuesday = 11
        case 6:  # Sunday
            days_since_preceding_tuesday = 12
    preceding_tuesday_date = a_date - \
        datetime.timedelta(days=days_since_preceding_tuesday)
    return preceding_tuesday_date.date()


def filter_and_split_cot_df_by_col_median_value(
    cot_df: pd.DataFrame,
    median_value: float,
    column_name: str
) -> NamedTuple:
    above_median_df = cot_df[cot_df[column_name]
                             >= median_value].copy().reset_index(drop=True)
    below_median_df = cot_df[cot_df[column_name]
                             < median_value].copy().reset_index(drop=True)
    column_split_by_median = namedtuple(
        'column_split_by_median', ['above_median_df', 'below_median_df'])
    return column_split_by_median(above_median_df, below_median_df)


def split_intraday_open_df_by_cot_median(
    cot_above_median_df: pd.DataFrame,
    cot_below_median_df: pd.DataFrame,
    intraday_open_df: pd.DataFrame
) -> NamedTuple:
    dates_cot_above_median = cot_above_median_df['Date'].dt.date.drop_duplicates(
    )
    dates_cot_below_median = cot_below_median_df['Date'].dt.date.drop_duplicates(
    )
    open_bars_where_cot_above_median_df = intraday_open_df[intraday_open_df['Date Of Preceding Tuesday'].isin(
        dates_cot_above_median)]
    open_bars_where_cot_below_median_df = intraday_open_df[intraday_open_df['Date Of Preceding Tuesday'].isin(
        dates_cot_below_median)]
    intraday_minute_bars_split = namedtuple('intraday_minute_bars_split', [
                                            'above_median_df', 'below_median_df'])
    return intraday_minute_bars_split(open_bars_where_cot_above_median_df, open_bars_where_cot_below_median_df)


def calculate_std_deviation_at_open_minute_offset(intraday_minute_bars:  NamedTuple, open_minute_offset: int) -> NamedTuple:
    '''
    Calculate the standard deviation of the "Price Change From Intraday Open" at n minutes after the open,
    for a given field split by above and below median
    '''
    above_median_df = intraday_minute_bars.above_median_df
    below_median_df = intraday_minute_bars.below_median_df
    above_median_at_open_minute_offset = above_median_df[
        above_median_df['Open Minutes Offset'] == open_minute_offset]
    below_median_at_open_minute_offset = below_median_df[
        below_median_df['Open Minutes Offset'] == open_minute_offset]
    above_median_standard_deviation = above_median_at_open_minute_offset['Price Change From Intraday Open'].std(
    )
    below_median_standard_deviation = below_median_at_open_minute_offset['Price Change From Intraday Open'].std(
    )
    intraday_price_change_standard_deviation = namedtuple('intraday_price_change_standard_deviation', [
        'above_median', 'below_median'])
    return intraday_price_change_standard_deviation(above_median_standard_deviation, below_median_standard_deviation)


def calculate_pct_above_below_intraday_open_price_change_at_open_minute_offset(
    intraday_minute_bars_df:  NamedTuple,
    open_minute_offset: int,
    intraday_price_cfo_at_key_minute_median: float
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
    pct_lt_median = round((num_rows_lt_median / total_series_length) * 100, 4)
    return {
        'pct_gte_median': pct_gte_median,
        'pct_lt_median': pct_lt_median
    }


def calculate_minmax_acfo(intraday_minute_bars:  NamedTuple) -> dict:
    above_median_df = intraday_minute_bars.above_median_df
    below_median_df = intraday_minute_bars.below_median_df
    open_minutes = [*range(0, 60, 1)]
    minute_of_max_above_median_mean_acfo = None
    minute_of_min_above_median_mean_acfo = None
    minute_of_max_below_median_mean_acfo = None
    minute_of_min_below_median_mean_acfo = None
    max_above_median_mean_acfo = None
    min_above_median_mean_acfo = None
    max_below_median_mean_acfo = None
    min_below_median_mean_acfo = None
    # Iteratve over each minute of the open and determine the max and min acfo and minute they occured
    # Do this for trading days that are both above and below the median of the CoT field respectively
    for minute in open_minutes:
        mean_acfo_above_median_at_minute = above_median_df[above_median_df['Open Minutes Offset']
                                                           == minute]['Price Change From Intraday Open'].mean()
        mean_acfo_below_median_at_minute = below_median_df[below_median_df['Open Minutes Offset']
                                                           == minute]['Price Change From Intraday Open'].mean()
        if (max_above_median_mean_acfo is None) or (mean_acfo_above_median_at_minute >= max_above_median_mean_acfo):
            max_above_median_mean_acfo = mean_acfo_above_median_at_minute
            minute_of_max_above_median_mean_acfo = minute
        if (min_above_median_mean_acfo is None) or (mean_acfo_above_median_at_minute <= min_above_median_mean_acfo):
            min_above_median_mean_acfo = mean_acfo_above_median_at_minute
            minute_of_min_above_median_mean_acfo = minute
        if (max_below_median_mean_acfo is None) or (mean_acfo_below_median_at_minute >= max_below_median_mean_acfo):
            max_below_median_mean_acfo = mean_acfo_below_median_at_minute
            minute_of_max_below_median_mean_acfo = minute
        if (min_below_median_mean_acfo is None) or (mean_acfo_below_median_at_minute <= min_below_median_mean_acfo):
            min_below_median_mean_acfo = mean_acfo_below_median_at_minute
            minute_of_min_below_median_mean_acfo = minute
    minmax_acfo_stats = {
        'minute_of_max_above_median_mean_acfo': minute_of_max_above_median_mean_acfo,
        'minute_of_min_above_median_mean_acfo': minute_of_min_above_median_mean_acfo,
        'minute_of_max_below_median_mean_acfo': minute_of_max_below_median_mean_acfo,
        'minute_of_min_below_median_mean_acfo': minute_of_min_below_median_mean_acfo,
        'max_above_median_mean_acfo': max_above_median_mean_acfo,
        'min_above_median_mean_acfo': min_above_median_mean_acfo,
        'max_below_median_mean_acfo': max_below_median_mean_acfo,
        'min_below_median_mean_acfo': min_below_median_mean_acfo
    }
    return minmax_acfo_stats


def calculate_average_intraday_price_change_grouped_by_open_minutes_offset(
    intraday_minute_bars:  NamedTuple
) -> pd.DataFrame:
    '''
    Group the intraday minute bars by their Open Minutes Offset and calculate the mean for each minute. Return all that as a single dataframe
    '''
    intraday_above_median_cot_field_df = intraday_minute_bars.above_median_df.groupby(
        'Open Minutes Offset', as_index=False)['Price Change From Intraday Open'].mean()
    intraday_below_median_cot_field_df = intraday_minute_bars.below_median_df.groupby(
        'Open Minutes Offset', as_index=False)['Price Change From Intraday Open'].mean()
    to_return_df = pd.DataFrame({
        'Open Minutes Offset': intraday_above_median_cot_field_df['Open Minutes Offset'],
        'Avg Intraday Price Change When COT Field Above Median': intraday_above_median_cot_field_df['Price Change From Intraday Open'],
        'Avg Intraday Price Change When COT Field Below Median': intraday_below_median_cot_field_df['Price Change From Intraday Open']
    })
    return to_return_df


def merge_dfs(list_of_dfs) -> pd.DataFrame:
    '''Combine a list of dataframes into one single one by concatting them together'''
    merged_df = initialize_cot_analytics_table_df()
    for a_report in list_of_dfs:
        merged_df = pd.concat([merged_df, a_report], ignore_index=True)
    return merged_df


def process_file(a_file: str, intraday_df: pd.DataFrame, open_type: str):
    '''Process all the fields in one COT report file'''
    cot_analytics_table_df = initialize_cot_analytics_table_df()
    csv_as_df = cot_csv_to_df(
        os.path.join(COMMITMENT_OF_TRADERS_REPORTS_BASE_PATH, a_file))
    columns = csv_as_df.columns.values.tolist()
    reportable_columns = list(filter(is_nonreportable_column, columns))
    intraday_price_cfo_at_key_minute_median = intraday_df[intraday_df['Open Minutes Offset'] ==
                                                          KEY_OPEN_MINUTE_OF_INTEREST - 1]['Price Change From Intraday Open'].median()
    for i in trange(len(reportable_columns)):
        a_column = reportable_columns[i]
        median_value_for_column = csv_as_df[a_column].median()
        cot_split_by_median = filter_and_split_cot_df_by_col_median_value(
            cot_df=csv_as_df, median_value=median_value_for_column, column_name=a_column)
        intraday_split_by_cot_df_median = split_intraday_open_df_by_cot_median(
            cot_above_median_df=cot_split_by_median.above_median_df,
            cot_below_median_df=cot_split_by_median.below_median_df,
            intraday_open_df=intraday_df
        )
        open_intraday_average_changes = calculate_average_intraday_price_change_grouped_by_open_minutes_offset(
            intraday_split_by_cot_df_median)
        intraday_price_change_standard_deviations = calculate_std_deviation_at_open_minute_offset(
            intraday_minute_bars=intraday_split_by_cot_df_median,
            open_minute_offset=KEY_OPEN_MINUTE_OF_INTEREST - 1
        )
        minmax_acfo_stats = calculate_minmax_acfo(
            intraday_minute_bars=intraday_split_by_cot_df_median)
        above_below_cfo_stats_for_above_cot_median = calculate_pct_above_below_intraday_open_price_change_at_open_minute_offset(
            intraday_minute_bars_df=intraday_split_by_cot_df_median.above_median_df,
            open_minute_offset=KEY_OPEN_MINUTE_OF_INTEREST - 1,
            intraday_price_cfo_at_key_minute_median=intraday_price_cfo_at_key_minute_median
        )
        above_below_cfo_stats_for_below_cot_median = calculate_pct_above_below_intraday_open_price_change_at_open_minute_offset(
            intraday_minute_bars_df=intraday_split_by_cot_df_median.below_median_df,
            open_minute_offset=KEY_OPEN_MINUTE_OF_INTEREST - 1,
            intraday_price_cfo_at_key_minute_median=intraday_price_cfo_at_key_minute_median
        )
        # Capture above median stats
        cot_analytics_table_df = cot_analytics_table_df.append({
            'Report Name': a_file[:-4],
            'Field Name': a_column,
            'Above/Below Median Of CoT Field': 'above',
            'Open Type': open_type,
            'Median Value Of CoT Field': median_value_for_column,
            'ACFO t+30': open_intraday_average_changes.iloc[29]['Avg Intraday Price Change When COT Field Above Median'],
            'ACFO t+60': open_intraday_average_changes.iloc[59]['Avg Intraday Price Change When COT Field Above Median'],
            f"Std Deviation of Intraday Price Change at Open t+{KEY_OPEN_MINUTE_OF_INTEREST}":
                intraday_price_change_standard_deviations.above_median,
            'Max ACFO': minmax_acfo_stats['max_above_median_mean_acfo'],
            'Min ACFO': minmax_acfo_stats['min_above_median_mean_acfo'],
            'Minute of Max ACFO': minmax_acfo_stats['minute_of_max_above_median_mean_acfo'],
            'Minute of Min ACFO': minmax_acfo_stats['minute_of_min_above_median_mean_acfo'],
            'Median Intraday CFO Value t+60': intraday_price_cfo_at_key_minute_median,
            'Percent GTE Median CFO t+60': above_below_cfo_stats_for_above_cot_median['pct_gte_median']
        }, ignore_index=True)
        # Now capture below median stats
        cot_analytics_table_df = cot_analytics_table_df.append({
            'Report Name': a_file[:-4],
            'Field Name': a_column,
            'Above/Below Median Of CoT Field': 'below',
            'Open Type': open_type,
            'Median Value Of CoT Field': median_value_for_column,
            'ACFO t+30': open_intraday_average_changes.iloc[29]['Avg Intraday Price Change When COT Field Below Median'],
            'ACFO t+60': open_intraday_average_changes.iloc[59]['Avg Intraday Price Change When COT Field Below Median'],
            f"Std Deviation of Intraday Price Change at Open t+{KEY_OPEN_MINUTE_OF_INTEREST}":
                intraday_price_change_standard_deviations.below_median,
            'Max ACFO': minmax_acfo_stats['max_below_median_mean_acfo'],
            'Min ACFO': minmax_acfo_stats['min_below_median_mean_acfo'],
            'Minute of Max ACFO': minmax_acfo_stats['minute_of_max_below_median_mean_acfo'],
            'Minute of Min ACFO': minmax_acfo_stats['minute_of_min_below_median_mean_acfo'],
            'Median Intraday CFO Value t+60': intraday_price_cfo_at_key_minute_median,
            'Percent GTE Median CFO t+60': above_below_cfo_stats_for_below_cot_median['pct_gte_median']
        }, ignore_index=True)
    return cot_analytics_table_df


def build_target_df() -> pd.DataFrame:
    '''Build out the target dataframe containing all data'''
    final_df = initialize_cot_analytics_table_df()
    csv_files = list_reportable_files(COMMITMENT_OF_TRADERS_REPORTS_BASE_PATH)
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
    print("Adding 'Date Of Preceding Tuesday' column to sliding open dataframe")
    intraday_sliding_open_df['Date Of Preceding Tuesday'] = intraday_sliding_open_df['DateTime'].apply(
        date_of_preceding_tuesday)
    print("Adding 'Date Of Preceding Tuesday' column to true open dataframe")
    intraday_true_open_df['Date Of Preceding Tuesday'] = intraday_true_open_df['DateTime'].apply(
        date_of_preceding_tuesday)
    print("Analyzing correlations between intraday open and cot reports for SLIDING OPEN")
    sliding_open_results_by_cot_reports = [
        process_file(
            a_file=a_file,
            intraday_df=intraday_sliding_open_df,
            open_type='sliding_open'
        )
        for a_file in csv_files
    ]
    # Merge the list of sliding open dataframes together (one associated with each COT report)
    merged_sliding_open_results_by_cot_df = merge_dfs(
        sliding_open_results_by_cot_reports)
    # Merge the merged datframe into the final one
    final_df = pd.concat(
        [final_df, merged_sliding_open_results_by_cot_df], ignore_index=True)
    print("Analyzing correlations between intraday open and cot reports for TRUE OPEN")
    true_open_results_by_cot_reports = [
        process_file(
            a_file=a_file,
            intraday_df=intraday_true_open_df,
            open_type='true_open'
        )
        for a_file in csv_files
    ]
    # Merge the list of true open dataframes together (one associated with each COT report)
    merged_true_open_results_by_cot_df = merge_dfs(
        true_open_results_by_cot_reports)
    # Merge the merged datframe into the final one
    final_df = pd.concat(
        [final_df, merged_true_open_results_by_cot_df], ignore_index=True)
    return final_df


# Script execution Starts Here
target_file_exists = os.path.exists(TARGET_FILE_DEST)
if target_file_exists:
    print('The target file already exists and will be overwritten. Abort in the next 5 seconds to cancel.')
    time.sleep(5)
target_df = build_target_df()
print(f"Saving target csv to {TARGET_FILE_DEST}")
target_df.to_csv(TARGET_FILE_DEST, index=False)

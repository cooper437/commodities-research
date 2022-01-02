from typing import List, NamedTuple
from collections import namedtuple
import os
import datetime
import operator
import pandas as pd
from tqdm import trange

CURRENT_DIR = os.path.dirname(__file__)
COMMITMENT_OF_TRADERS_REPORTS_BASE_PATH = os.path.join(
    CURRENT_DIR, '../../data/raw/nasdaq_data_link/commitment_of_trade/')
CONTRACT_INTRADAY_SLIDING_OPEN_FILE_PATH = os.path.join(
    CURRENT_DIR, '../../data/processed/futures_contracts/contract_open_enriched_sliding_open.csv')
CONTRACT_INTRADAY_TRUE_OPEN_FILE_PATH = os.path.join(
    CURRENT_DIR, '../../data/processed/futures_contracts/contract_open_enriched_true_open.csv')


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


def initialize_cot_analytics_table_df() -> pd.DataFrame:
    '''A dataframe that contains the structure needed for the analytics table that is output by the script'''
    columns = ['Report Name', 'Field Name', 'Open Type', 'Above/Below Median', 'Median Value', 'AFCO t=0',
               'AFCO t+5', 'AFCO t+15', 'AFCO t+30', 'AFCO t+40', 'AFCO t+50', 'AFCO t+60']
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


def process_file(a_file: str, intraday_df: pd.DataFrame, open_type: str):
    cot_analytics_table_df = initialize_cot_analytics_table_df()
    csv_as_df = cot_csv_to_df(
        os.path.join(COMMITMENT_OF_TRADERS_REPORTS_BASE_PATH, a_file))
    columns = csv_as_df.columns.values.tolist()
    reportable_columns = list(filter(is_nonreportable_column, columns))
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
        # Capture above median stats
        cot_analytics_table_df = cot_analytics_table_df.append({
            'Report Name': a_file[:-4],
            'Field Name': a_column,
            'Above/Below Median': 'above',
            'Open Type': open_type,
            'Median Value': median_value_for_column,
            'AFCO t=0': open_intraday_average_changes.iloc[0]['Avg Intraday Price Change When COT Field Above Median'],
            'AFCO t+5': open_intraday_average_changes.iloc[4]['Avg Intraday Price Change When COT Field Above Median'],
            'AFCO t+15': open_intraday_average_changes.iloc[14]['Avg Intraday Price Change When COT Field Above Median'],
            'AFCO t+30': open_intraday_average_changes.iloc[29]['Avg Intraday Price Change When COT Field Above Median'],
            'AFCO t+40': open_intraday_average_changes.iloc[39]['Avg Intraday Price Change When COT Field Above Median'],
            'AFCO t+50': open_intraday_average_changes.iloc[49]['Avg Intraday Price Change When COT Field Above Median'],
            'AFCO t+60': open_intraday_average_changes.iloc[59]['Avg Intraday Price Change When COT Field Above Median']
        }, ignore_index=True)
        # Now capture below median stats
        cot_analytics_table_df = cot_analytics_table_df.append({
            'Report Name': a_file[:-4],
            'Field Name': a_column,
            'Above/Below Median': 'below',
            'Open Type': open_type,
            'Median Value': median_value_for_column,
            'AFCO t=0': open_intraday_average_changes.iloc[0]['Avg Intraday Price Change When COT Field Below Median'],
            'AFCO t+5': open_intraday_average_changes.iloc[4]['Avg Intraday Price Change When COT Field Below Median'],
            'AFCO t+15': open_intraday_average_changes.iloc[14]['Avg Intraday Price Change When COT Field Below Median'],
            'AFCO t+30': open_intraday_average_changes.iloc[29]['Avg Intraday Price Change When COT Field Below Median'],
            'AFCO t+40': open_intraday_average_changes.iloc[39]['Avg Intraday Price Change When COT Field Below Median'],
            'AFCO t+50': open_intraday_average_changes.iloc[49]['Avg Intraday Price Change When COT Field Below Median'],
            'AFCO t+60': open_intraday_average_changes.iloc[59]['Avg Intraday Price Change When COT Field Below Median']
        }, ignore_index=True)
    return cot_analytics_table_df


final_df = initialize_cot_analytics_table_df()
csv_files = list_reportable_files(COMMITMENT_OF_TRADERS_REPORTS_BASE_PATH)
intraday_sliding_open_df = intraday_open_csv_to_df(
    CONTRACT_INTRADAY_SLIDING_OPEN_FILE_PATH)
intraday_true_open_df = intraday_open_csv_to_df(
    CONTRACT_INTRADAY_TRUE_OPEN_FILE_PATH)
intraday_true_open_df['Date Of Preceding Tuesday'] = intraday_true_open_df['DateTime'].apply(
    date_of_preceding_tuesday)
intraday_sliding_open_df['Date Of Preceding Tuesday'] = intraday_sliding_open_df['DateTime'].apply(
    date_of_preceding_tuesday)

sliding_open_results_by_cot_reports = [
    process_file(
        a_file=a_file,
        intraday_df=intraday_sliding_open_df,
        open_type='sliding_open'
    )
    for a_file in csv_files
]
for a_report in sliding_open_results_by_cot_reports:
    final_df = pd.concat([final_df, a_report], ignore_index=True)

true_open_results_by_cot_reports = [
    process_file(
        a_file=a_file,
        intraday_df=intraday_true_open_df,
        open_type='true_open'
    )
    for a_file in csv_files
]
for a_report in true_open_results_by_cot_reports:
    final_df = pd.concat([final_df, a_report], ignore_index=True)
print(final_df)

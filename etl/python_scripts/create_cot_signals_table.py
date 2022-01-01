from typing import List, NamedTuple
from collections import namedtuple
import os
import operator
import pandas as pd

CURRENT_DIR = os.path.dirname(__file__)
COMMITMENT_OF_TRADERS_REPORTS_BASE_PATH = os.path.join(
    CURRENT_DIR, '../../data/raw/nasdaq_data_link/commitment_of_trade/')


def list_reportable_files(base_path: str) -> List[str]:
    # Get a list of all the csv files to process in this script
    csv_files = []
    for file in os.listdir(base_path):
        if file.endswith(".csv"):
            csv_files.append(file)
    csv_files.sort()
    return csv_files


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


def filter_and_split_df_by_col_median_value(
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


def process_file(a_file: str):
    csv_as_df = cot_csv_to_df(
        os.path.join(COMMITMENT_OF_TRADERS_REPORTS_BASE_PATH, a_file))
    columns = csv_as_df.columns.values.tolist()
    reportable_columns = list(filter(is_nonreportable_column, columns))
    for a_column in reportable_columns:
        median_value_for_column = csv_as_df[a_column].median()
        filter_and_split_df_by_col_median_value(
            cot_df=csv_as_df, median_value=median_value_for_column, column_name=a_column)
        print(median_value_for_column)


csv_files = list_reportable_files(COMMITMENT_OF_TRADERS_REPORTS_BASE_PATH)
results = [process_file(a_file) for a_file in csv_files]

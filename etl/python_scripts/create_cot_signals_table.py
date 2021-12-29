from typing import List
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


def is_irrelevant_column(a_column):
    irrelevant_columns = [
        'Date', '% OF Open Interest (OI) All NoCIT', 'Open Interest - % of OI']
    is_irrelevant_col = operator.not_(any(
        element == a_column for element in irrelevant_columns))
    return is_irrelevant_col


csv_files = list_reportable_files(COMMITMENT_OF_TRADERS_REPORTS_BASE_PATH)
for a_file in csv_files:
    csv_as_df = cot_csv_to_df(
        os.path.join(COMMITMENT_OF_TRADERS_REPORTS_BASE_PATH, a_file))
    columns = csv_as_df.columns.values.tolist()
    relevant_columns = list(filter(is_irrelevant_column, columns))
    print(relevant_columns)

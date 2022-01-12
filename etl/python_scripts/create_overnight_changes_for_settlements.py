import enum
import os
import pandas as pd
from typing import List, Tuple
from pandas.core.frame import DataFrame
from tqdm import trange
from datetime import datetime, timedelta
import logging
import sys
import getopt

CURRENT_DIR = os.path.dirname(__file__)
RAW_DATA_DIR = os.path.join(
    CURRENT_DIR, '../../data/raw/nasdaq_srf_futures_settlement')
LIVE_CATTLE_INTRADAY_TRUE_OPEN_FILE_PATH = os.path.join(
    CURRENT_DIR, '../../data/processed/futures_contracts/contract_open_enriched_true_open.csv'
)
LIVE_CATTLE_INTRADAY_SLIDING_OPEN_FILE_PATH = os.path.join(
    CURRENT_DIR, '../../data/processed/futures_contracts/contract_open_enriched_sliding_open.csv'
)

logging.basicConfig(
    format='%(asctime)s - %(levelname)s:%(message)s', level=logging.DEBUG)


def settlement_csv_files_to_analyze(data_dir):
    # Get a list of all the csv files to process
    csv_files = []
    for file in os.listdir(RAW_DATA_DIR):
        csv_files.append(file)
    csv_files.sort()
    return csv_files


def intraday_open_to_df(filename) -> pd.DataFrame:
    csv_as_df = pd.read_csv(filename,
                            parse_dates=['DateTime'], usecols=['Symbol', 'DateTime', 'Open Minutes Offset',
                                                               'Open', 'High', 'Low', 'Close', 'Volume',
                                                               'Price Change From Intraday Open', 'Expiration Date', 'DTE'
                                                               ]
                            )
    return csv_as_df


def settlement_data_to_df(filename) -> pd.DataFrame:
    csv_as_df = pd.read_csv(os.path.join(RAW_DATA_DIR, filename),
                            parse_dates=['Date'], usecols=['Date', 'Open', 'High', 'Low', 'Settle', 'Volume', 'Prev. Day Open Interest'])
    return csv_as_df


def contract_month_and_year_from_file_name(filename: str) -> Tuple[str]:
    settlement_data_month_and_year = filename[-9:-4]
    settlement_data_contract_month = settlement_data_month_and_year[0]
    settlement_data_contract_year = settlement_data_month_and_year[1:]
    return settlement_data_contract_month + settlement_data_contract_year[-2:]


def process_overnight_settlement_changes(settlement_csv_filenames: List[str], intraday_open_df: pd.DataFrame):
    for item_index in trange(len(settlement_csv_filenames)):
        filename = settlement_csv_filenames[item_index]
        contract_month_and_year = contract_month_and_year_from_file_name(
            filename)
        settlement_data_df = settlement_data_to_df(filename)
        this_contracts_open_data = intraday_open_df[intraday_open_df['Symbol'].str[-3:]
                                                    == contract_month_and_year]
        logging.info("hello")


settlement_csv_filenames = settlement_csv_files_to_analyze(RAW_DATA_DIR)
logging.info(f"Parsing intraday true open into dataframe")
intrday_true_open_df = intraday_open_to_df(
    LIVE_CATTLE_INTRADAY_TRUE_OPEN_FILE_PATH)
logging.info(f"Parsing intraday sliding open into dataframe")
intraday_true_open_df = intraday_open_to_df(
    LIVE_CATTLE_INTRADAY_SLIDING_OPEN_FILE_PATH)
process_overnight_settlement_changes(
    settlement_csv_filenames=settlement_csv_filenames, intraday_open_df=intraday_true_open_df)

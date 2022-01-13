import enum
import os
import pandas as pd
import datetime
from typing import List, Tuple
from pandas.core.frame import DataFrame
from tqdm import trange
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


def calculate_overnight_change_from_settlement(a_days_open_bar, a_prior_days_settlement_bar):
    # we are missing an open price or prior day settlement price
    if a_days_open_bar is None or a_prior_days_settlement_bar is None:
        return None
    price_change = a_days_open_bar['Open'] - \
        a_prior_days_settlement_bar['Settle']
    return price_change


def get_first_bar_available_for_day(a_date: datetime.date, a_contracts_open_data: pd.DataFrame) -> pd.Series:
    minute_bars_for_day = a_contracts_open_data[a_contracts_open_data['DateTime'].dt.date == a_date]
    minute_bars_for_day_sorted = minute_bars_for_day.sort_values(
        by="Open Minutes Offset", ascending=True).reset_index()
    first_avail_bar = minute_bars_for_day_sorted.iloc[0]
    return first_avail_bar


def get_settlement_data_for_date(a_date: datetime.date, settlement_data_df: pd.DataFrame) -> pd.Series:
    data_for_date = settlement_data_df[settlement_data_df['Date'].dt.date == a_date]
    number_of_rows = len(data_for_date.index)
    if number_of_rows == 0:
        return None
    if number_of_rows > 1:
        raise Exception('More than one row of settlement data matched')
    return data_for_date.iloc[0]


def process_overnight_settlement_changes(settlement_csv_filenames: List[str], intraday_open_df: pd.DataFrame):
    overnight_settlement_price_changes_df = pd.DataFrame(
        columns=['Date', 'Symbol', 'Price Difference b/w Open And Prior Day Settlement'])
    # Process each table of settlement data - one for each contract
    for item_index in trange(len(settlement_csv_filenames)):
        filename = settlement_csv_filenames[item_index]
        contract_month_and_year = contract_month_and_year_from_file_name(
            filename)
        settlement_data_df = settlement_data_to_df(filename)
        a_contracts_open_data = intraday_open_df[intraday_open_df['Symbol'].str[-3:]
                                                 == contract_month_and_year]
        a_contracts_trading_dates = a_contracts_open_data['DateTime'].dt.date\
            .drop_duplicates().to_list()
        # Determine the price change between open and prior day settlement for each trading day in the contract
        for a_date in a_contracts_trading_dates:
            first_available_bar = get_first_bar_available_for_day(
                a_date=a_date, a_contracts_open_data=a_contracts_open_data)
            settlement_bar = get_settlement_data_for_date(
                a_date=a_date, settlement_data_df=settlement_data_df)
            difference_between_open_price_and_prior_day_settlement = calculate_overnight_change_from_settlement(
                a_days_open_bar=first_available_bar, a_prior_days_settlement_bar=settlement_bar)
            overnight_settlement_price_changes_df = overnight_settlement_price_changes_df.append(
                {'Symbol': 'LE' + contract_month_and_year, 'Date': a_date, 'Price Difference b/w Open And Prior Day Settlement': difference_between_open_price_and_prior_day_settlement}, ignore_index=True)
    return overnight_settlement_price_changes_df


settlement_csv_filenames = settlement_csv_files_to_analyze(RAW_DATA_DIR)
logging.info(f"Parsing intraday true open into dataframe")
intraday_true_open_df = intraday_open_to_df(
    LIVE_CATTLE_INTRADAY_TRUE_OPEN_FILE_PATH)
logging.info(f"Parsing intraday sliding open into dataframe")
intraday_sliding_open_df = intraday_open_to_df(
    LIVE_CATTLE_INTRADAY_SLIDING_OPEN_FILE_PATH)
overnight_changes_true_open_df = process_overnight_settlement_changes(
    settlement_csv_filenames=settlement_csv_filenames, intraday_open_df=intraday_true_open_df)
overnight_changes_sliding_open_df = process_overnight_settlement_changes(
    settlement_csv_filenames=settlement_csv_filenames, intraday_open_df=intraday_sliding_open_df)
logging.info('Done')

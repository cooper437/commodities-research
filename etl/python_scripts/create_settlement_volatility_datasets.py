import enum
import os
import numpy as np
import pandas as pd
import datetime
from typing import List, Tuple
from decimal import Decimal, ROUND_HALF_UP
from tqdm import trange
import logging
import math
import sys
import getopt

CURRENT_DIR = os.path.dirname(__file__)
RAW_DATA_DIR = os.path.join(
    CURRENT_DIR, '../../data/raw/nasdaq_srf_futures_settlement')
PROCESSED_DATA_DIR = os.path.join(
    CURRENT_DIR, '../../data/processed/futures_contracts')
SETTLEMENT_DATA_DIR = os.path.join(PROCESSED_DATA_DIR, 'settlement_analytics')

logging.basicConfig(
    format='%(asctime)s - %(levelname)s:%(message)s', level=logging.INFO)


def settlement_csv_files_to_analyze(data_dir):
    # Get a list of all the csv files to process
    csv_files = []
    for file in os.listdir(RAW_DATA_DIR):
        csv_files.append(file)
    csv_files.sort()
    return csv_files
# Date,Open,High,Low,Settle,Volume,Prev. Day Open Interest


def settlement_data_to_df(filename) -> pd.DataFrame:
    csv_as_df = pd.read_csv(os.path.join(RAW_DATA_DIR, filename),
                            parse_dates=['Date'], usecols=['Date', 'Open', 'High', 'Low', 'Settle', 'Volume', 'Prev. Day Open Interest'])
    return csv_as_df


def round_to_nearest_thousandth(a_float: float) -> Decimal:
    rounded = Decimal(a_float).quantize(
        Decimal('0.001'), rounding=ROUND_HALF_UP)
    return rounded


def contract_month_and_year_from_file_name(filename: str) -> str:
    settlement_data_month_and_year = filename[-9:-4]
    settlement_data_contract_month = settlement_data_month_and_year[0]
    settlement_data_contract_year = settlement_data_month_and_year[1:]
    return settlement_data_contract_month + settlement_data_contract_year[-2:]


def calc_custom_std_deviation(a_price_series: pd.Series) -> int:
    distance_to_zero_squared_series = a_price_series.apply(
        lambda x: pow(abs(x), 2))
    mean_distance_to_zero_squared_series = distance_to_zero_squared_series.mean()
    square_root = math.sqrt(mean_distance_to_zero_squared_series)
    return round_to_nearest_thousandth(square_root)


def process_settlement_volatility(
    settlement_csv_filenames: List[str],
):
    all_dates_metadata = []
    for item_index in trange(len(settlement_csv_filenames)):
        filename = settlement_csv_filenames[item_index]
        contract_month_and_year = contract_month_and_year_from_file_name(
            filename)
        settlement_data_df = settlement_data_to_df(filename)
        settlement_data_df = settlement_data_df.sort_values(
            by="Date", ascending=True, ignore_index=True)
        first_date_for_contract = settlement_data_df.iloc[0]['Date'].date()
        for index, row in settlement_data_df.iterrows():
            a_date = row['Date'].date()
            a_date_metadata = {
                'Date': a_date,
                'Symbol': 'LE' + contract_month_and_year
            }
            a_date_30_days_prior = a_date - datetime.timedelta(days=30)
            within_last_thirty_days_df = settlement_data_df[(settlement_data_df['Date'].dt.date >= a_date_30_days_prior) & (
                settlement_data_df['Date'].dt.date <= a_date)]
            a_date_metadata['30D Count'] = len(
                within_last_thirty_days_df.index)
            # Skip calculting the 30 day CSD for the first thirty days of settlement data for every contract
            if (a_date - first_date_for_contract).days <= 30:
                a_date_metadata['30D CSD'] = None
            else:
                csd_thirty = calc_custom_std_deviation(
                    within_last_thirty_days_df['Settle'])
                a_date_metadata['30D CSD'] = csd_thirty
            all_dates_metadata.append(a_date_metadata)
    print('hello')


settlement_csv_filenames = settlement_csv_files_to_analyze(RAW_DATA_DIR)
process_settlement_volatility(settlement_csv_filenames)

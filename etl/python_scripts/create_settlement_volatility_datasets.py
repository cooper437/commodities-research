'''
Generate an analytics table of potential trading signals based on the NASDAQ SRF settlement data.
The table that is output contains a custom standard deviation, range, and count values related to data series for
7 days, 30 days, and 365 days back.
'''
import os
import pandas as pd
import datetime
from typing import List, Tuple
from decimal import Decimal, ROUND_HALF_UP
from tqdm import trange
import logging
import time
import math

CURRENT_DIR = os.path.dirname(__file__)
RAW_DATA_DIR = os.path.join(
    CURRENT_DIR, '../../data/raw/nasdaq_srf_futures_settlement')
PROCESSED_DATA_DIR = os.path.join(
    CURRENT_DIR, '../../data/processed/futures_contracts')
SETTLEMENT_DATA_DIR = os.path.join(PROCESSED_DATA_DIR, 'settlement_analytics')
TARGET_FILENAME = 'settlement_volatility.csv'
TARGET_FILE_DEST = os.path.join(SETTLEMENT_DATA_DIR, TARGET_FILENAME)

logging.basicConfig(
    format='%(asctime)s - %(levelname)s:%(message)s', level=logging.INFO)


def settlement_csv_files_to_analyze(data_dir):
    # Get a list of all the csv files to process
    csv_files = []
    for file in os.listdir(RAW_DATA_DIR):
        csv_files.append(file)
    csv_files.sort()
    return csv_files


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


def calc_custom_std_deviation(a_price_series: pd.Series) -> Decimal:
    distance_to_zero_squared_series = a_price_series.apply(
        lambda x: pow(abs(x), 2))
    mean_distance_to_zero_squared_series = distance_to_zero_squared_series.mean()
    square_root = math.sqrt(mean_distance_to_zero_squared_series)
    return round_to_nearest_thousandth(square_root)


def calc_range_for_series(a_price_series: pd.Series) -> Decimal:
    a_max = a_price_series.max()
    a_min = a_price_series.min()
    a_range = round_to_nearest_thousandth(a_max - a_min)
    return a_range


def calc_thirty_day_stats(
    within_last_thirty_days_df: pd.DataFrame,
    a_date: datetime.date,
    first_date_for_contract: datetime.date,
    a_dates_settlement_data: pd.Series,
    thirty_days_plus_one_settlement_data: pd.Series
) -> Tuple:
    # Skip calculating the stats for the first n days where a_date - n days takes us before the first trading day available
    if (a_date - first_date_for_contract).days < 30:
        csd = None
        a_range = None
    else:
        csd_settlement_prices_series = within_last_thirty_days_df['Settle']
        # if we have the 31st days data insert it at the beginning of the list
        if thirty_days_plus_one_settlement_data is not None:
            csd_settlement_prices_series = pd.concat([pd.Series(
                [thirty_days_plus_one_settlement_data['Settle']]), csd_settlement_prices_series], ignore_index=True)
        settlement_price_change_series = csd_settlement_prices_series.diff()[
            1:]
        csd = calc_custom_std_deviation(
            settlement_price_change_series)
        a_range = calc_range_for_series(
            within_last_thirty_days_df['Settle'])
    return (a_range, csd)


def calc_seven_day_stats(
    within_last_seven_days_df: pd.DataFrame,
    a_date: datetime.date,
    first_date_for_contract: datetime.date
) -> Decimal:
    # Skip calculating the stats for the first n days where a_date - n days takes us before the first trading day available
    if (a_date - first_date_for_contract).days < 7:
        a_range = None
    else:
        a_range = calc_range_for_series(
            within_last_seven_days_df['Settle'])
    return a_range


def calc_one_year_stats(
    within_last_year_df: pd.DataFrame,
    a_date: datetime.date,
    first_date_for_contract: datetime.date
) -> Decimal:
    # Skip calculating the stats for the first n days where a_date - n days takes us before the first trading day available
    if (a_date - first_date_for_contract).days < 365:
        a_range = None
    else:
        a_range = calc_range_for_series(
            within_last_year_df['Settle'])
    return a_range


def get_index_of_thirty_one_days_ago(within_last_thirty_days_df: pd.DataFrame):
    '''
    Get the index of the day just outside our 30 day sliding window if we have it
    '''
    if (len(within_last_thirty_days_df.index)) == 0:
        return None
    index_value = within_last_thirty_days_df.index.values.min() - 1
    return index_value if index_value >= 0 else None


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
            a_date_year_prior = a_date - datetime.timedelta(days=365)
            a_date_30_days_prior = a_date - datetime.timedelta(days=30)
            a_date_7_days_prior = a_date - datetime.timedelta(days=7)
            within_last_year_df = settlement_data_df[(settlement_data_df['Date'].dt.date >= a_date_year_prior) & (
                settlement_data_df['Date'].dt.date < a_date)]
            within_last_thirty_days_df = within_last_year_df[(within_last_year_df['Date'].dt.date >= a_date_30_days_prior) & (
                within_last_year_df['Date'].dt.date < a_date)]
            within_last_seven_days_df = within_last_thirty_days_df[(within_last_thirty_days_df['Date'].dt.date >= a_date_7_days_prior) & (
                within_last_thirty_days_df['Date'].dt.date < a_date)]
            a_date_metadata['30D Count'] = len(
                within_last_thirty_days_df.index)
            a_date_metadata['7D Count'] = len(
                within_last_seven_days_df.index)
            a_date_metadata['365D Count'] = len(
                within_last_year_df.index)
            a_date_metadata['365D Range'] = calc_range_for_series(
                within_last_year_df['Settle'])
            a_date_metadata['7D Range'] = calc_range_for_series(
                within_last_seven_days_df['Settle'])
            index_of_thirty_one_days_ago = get_index_of_thirty_one_days_ago(
                within_last_thirty_days_df)
            # if we have settlement data for 31 days ago set it. This is needed to ensure that our CSD calcs return correctly even for the 30th day
            if (index_of_thirty_one_days_ago is not None):
                thirty_days_plus_one_settlement_data = settlement_data_df.iloc[
                    index_of_thirty_one_days_ago]
            else:
                thirty_days_plus_one_settlement_data = None
            (thirty_day_range, csd_thirty) = calc_thirty_day_stats(
                within_last_thirty_days_df=within_last_thirty_days_df,
                a_date=a_date,
                first_date_for_contract=first_date_for_contract,
                a_dates_settlement_data=row,
                thirty_days_plus_one_settlement_data=thirty_days_plus_one_settlement_data
            )
            seven_day_range = calc_seven_day_stats(
                within_last_seven_days_df=within_last_seven_days_df,
                a_date=a_date,
                first_date_for_contract=first_date_for_contract
            )
            one_year_range = calc_one_year_stats(
                within_last_year_df=within_last_year_df,
                a_date=a_date,
                first_date_for_contract=first_date_for_contract
            )
            a_date_metadata['30D CSD'] = csd_thirty
            a_date_metadata['30D Range'] = thirty_day_range
            a_date_metadata['7D Range'] = seven_day_range
            a_date_metadata['365D Range'] = one_year_range
            all_dates_metadata.append(a_date_metadata)
    return pd.DataFrame(all_dates_metadata)


# Script execution Starts Here
target_file_exists = os.path.exists(TARGET_FILE_DEST)
if target_file_exists:
    logging.info(
        'The target file already exists and will be overwritten. Abort in the next 5 seconds to cancel.')
    time.sleep(5)
settlement_csv_filenames = settlement_csv_files_to_analyze(RAW_DATA_DIR)
volatility_stats_df = process_settlement_volatility(settlement_csv_filenames)
logging.info(f"Saving target csv to {TARGET_FILE_DEST}")
volatility_stats_df.to_csv(TARGET_FILE_DEST, index=False)

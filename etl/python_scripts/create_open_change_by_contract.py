import os
import pandas as pd
from tqdm import trange
from datetime import datetime

CONTRACTS_PREFIX_MATCHER = 'LEG'  # Option limit if desired
CURRENT_DIR = os.path.dirname(__file__)
RAW_DATA_DIR = os.path.join(
    CURRENT_DIR, '../../data/raw/firstratedata_futures')
PROCESSED_DATA_DIR = os.path.join(
    CURRENT_DIR, '../../data/processed/futures_contracts')
# The date that the pit open time changed
DATE_OF_PIT_OPEN_CHANGE = datetime(2015, 7, 2)
# How many minutes from the contract open we consider to be the open window
WIDTH_TRADING_WINDOW_OPEN_MINUTES = 60


def convert_csv_to_df(filename):
    csv_as_df = pd.read_csv(
        f"{RAW_DATA_DIR}/{filename}",
        parse_dates=['DateTime'], usecols=['DateTime', 'Open', 'High', 'Low', 'Close', 'Volume']
    )
    return csv_as_df


def csv_files_to_analyze(data_dir, filename_prefix_matcher):
    # Get a list of all the csv files to process
    csv_files = []
    for file in os.listdir(RAW_DATA_DIR):
        if file.startswith(CONTRACTS_PREFIX_MATCHER):
            csv_files.append(file)
    csv_files.sort()
    return csv_files


def contract_open_time(trading_bar_datetime: datetime):
    '''
    Given the date of a trading bar return the time of day of the open for that same date
    This helps account for the change in open time after the pit closed on 7/2/2015
    '''
    if trading_bar_datetime >= DATE_OF_PIT_OPEN_CHANGE:
        return trading_bar_datetime.replace(hour=8, minute=30, second=0, microsecond=0)
    else:
        return trading_bar_datetime.replace(hour=9, minute=5, second=0, microsecond=0)


def calculate_minutes_after_open(trading_bar_datetime: datetime, contract_open_datetime: datetime):
    if trading_bar_datetime <= contract_open_datetime:
        delta_with_open = contract_open_datetime - trading_bar_datetime
        delta_with_open_minutes = int(delta_with_open.seconds / 60)
        delta_with_open_minutes = -delta_with_open_minutes
    else:
        delta_with_open = trading_bar_datetime - contract_open_datetime
        delta_with_open_minutes = int(delta_with_open.seconds / 60)
    # print(
    #     f"The difference between trading_bar_datetime={trading_bar_datetime} and contract_open_datetime={contract_open_datetime} is {delta_with_open_minutes} minutes")
    return delta_with_open_minutes


def filter_rows_outside_open


initial_df = pd.DataFrame(
    columns=['Symbol', 'DateTime', 'Minutes From Open' 'Open', 'High', 'Low', 'Close', 'Volume'])
csv_files = csv_files_to_analyze(
    data_dir=RAW_DATA_DIR, filename_prefix_matcher=CONTRACTS_PREFIX_MATCHER)
print(f"Analyzing {len(csv_files)} the files")
for item in trange(len(csv_files)):
    file = csv_files[item]
    contract_symbol = file[0:-4]
    contract_df = convert_csv_to_df(file)
    minutes_after_open = contract_df.apply(
        lambda row: calculate_minutes_after_open(
            trading_bar_datetime=row['DateTime'], contract_open_datetime=contract_open_time(row['DateTime'])),
        axis=1
    )
    contract_df['Minutes From Open'] = minutes_after_open
    contract_df['Symbol'] = contract_symbol
    # trading_bar_datetime = datetime(2009, 1, 14, 8, 58)
    # contract_open_datetime = datetime(2009, 1, 14, 9, 5)
    # calculate_minutes_after_open(trading_bar_datetime, contract_open_datetime)
    # trading_bar_datetime = contract_df['DateTime'][0]
    # contract_open_datetime = contract_open_time(trading_bar_datetime)
    # minutes_after_open(trading_bar_datetime, contract_open_datetime)
    print('next')

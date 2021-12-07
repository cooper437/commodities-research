'''
Transform our futures contracts dataset from First Rate Data into a csv table
that contains just the opening window of trading activity for each contract and day.
The raw data is also enriched to include the following fields:
Symbol,DateTime,Open Minutes Offset,Open,High,Low,Close,Volume,Expiration Date,DTE
Finally we save this file to disk as a csv
'''
import os
import pandas as pd
from tqdm import trange
from datetime import datetime

CONTRACTS_PREFIX_MATCHER = 'LE'  # Optional limit if desired
CURRENT_DIR = os.path.dirname(__file__)
RAW_DATA_DIR = os.path.join(
    CURRENT_DIR, '../../data/raw/firstratedata_futures')
PROCESSED_DATA_DIR = os.path.join(
    CURRENT_DIR, '../../data/processed/futures_contracts')
EXPIRATION_DATE_BY_CONTRACT_CSV_FILEPATH = os.path.join(
    PROCESSED_DATA_DIR, 'expiration_date_by_contract.csv')
# The date that the pit open time changed
DATE_OF_PIT_OPEN_CHANGE = datetime(2015, 7, 2)
# How many minutes from the contract open we consider to be the open window
WIDTH_TRADING_WINDOW_OPEN_MINUTES = 60
TARGET_FILENAME = 'contract_open_enriched.csv'
TARGET_FILE_DEST = os.path.join(PROCESSED_DATA_DIR, TARGET_FILENAME)


def convert_expiration_date_by_contract_df(filename):
    csv_as_df = pd.read_csv(
        filename,
        usecols=['Symbol', 'Expiration Date']
    )
    return csv_as_df


def convert_contract_csv_to_df(filename):
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
    if trading_bar_datetime >= DATE_OF_PIT_OPEN_CHANGE:  # Trading bar is after the change to pit open
        return trading_bar_datetime.replace(hour=9, minute=30, second=0, microsecond=0)
    else:  # Trading bar is before the change to pit open
        return trading_bar_datetime.replace(hour=10, minute=5, second=0, microsecond=0)


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


def filter_rows_outside_open(contract_df) -> pd.DataFrame:
    '''Given a dataframe for a contract return a copy that has the rows that are outside the open window filtered out'''
    is_inside_open_winow = (
        (contract_df['Open Minutes Offset'] >= 0) & (contract_df['Open Minutes Offset'] <= 60))
    filtered_rows = contract_df[is_inside_open_winow]
    return filtered_rows


def calculate_dte(trading_bar_datetime: datetime, contract_expiration_datetime: datetime):
    trading_bar_datetime_beginning = trading_bar_datetime.replace(
        hour=0, minute=0)  # Zero out the seconds and minutes to avoid negative values
    delta_to_expiration = contract_expiration_datetime - trading_bar_datetime_beginning
    return delta_to_expiration.days


def get_open_bar_for_same_day(trading_minute_bar: pd.Series, a_contract_open_df: pd.DataFrame) -> pd.Series:
    bar_datetime = trading_minute_bar['DateTime']
    intraday_open_bar_datetime = contract_open_time(bar_datetime)
    intraday_open_bar_df = a_contract_open_df[a_contract_open_df['DateTime']
                                              == intraday_open_bar_datetime]
    # When there is no bar available at the actual open time
    # instead we use the first bar available for the same day
    if len(intraday_open_bar_df) == 0:  # There is no bar available at the actual open time
        bar_date_only = bar_datetime.date()
        # All the bars from the same day
        same_day_bars_only = a_contract_open_df[a_contract_open_df['DateTime'].dt.date == bar_date_only]
        # Sorted ascending
        same_day_bars_only_sorted = same_day_bars_only.sort_values(
            by=['DateTime'], ascending=True)
        # Return the 1st one we have as though its the open
        return same_day_bars_only_sorted.iloc[0]
    # There is a bar available at the actual open time so return it
    return intraday_open_bar_df.iloc[0]


def calculate_intraday_open_price_change(trading_minute_bar: pd.Series, open_bar_for_same_day: pd.Series):
    if open_bar_for_same_day is None:
        return None
    close_price_of_bar = trading_minute_bar['Close']
    open_price_of_day = open_bar_for_same_day['Open']
    return format(close_price_of_bar - open_price_of_day, '.3f')


enriched_contract_open_df = pd.DataFrame(
    columns=['Symbol', 'DateTime', 'Open Minutes Offset', 'Open', 'High', 'Low', 'Close', 'Volume'])
csv_files = csv_files_to_analyze(
    data_dir=RAW_DATA_DIR, filename_prefix_matcher=CONTRACTS_PREFIX_MATCHER)
print(f"Analyzing {len(csv_files)} files")
expiration_by_contract_df = convert_expiration_date_by_contract_df(
    EXPIRATION_DATE_BY_CONTRACT_CSV_FILEPATH)
for item in trange(len(csv_files)):
    file = csv_files[item]
    contract_symbol = file[0:-4]
    contract_df = convert_contract_csv_to_df(file)
    minutes_after_open = contract_df.apply(
        lambda row: calculate_minutes_after_open(
            trading_bar_datetime=row['DateTime'], contract_open_datetime=contract_open_time(row['DateTime'])),
        axis=1
    )
    contract_df['Open Minutes Offset'] = minutes_after_open
    contract_df['Symbol'] = contract_symbol
    filtered_contract_df = filter_rows_outside_open(contract_df).copy()
    price_change_from_open_bar_series = filtered_contract_df.apply(
        lambda row: calculate_intraday_open_price_change(trading_minute_bar=row, open_bar_for_same_day=get_open_bar_for_same_day(
            trading_minute_bar=row, a_contract_open_df=filtered_contract_df)),
        axis=1
    )
    filtered_contract_df['Price Change From Intraday Open'] = price_change_from_open_bar_series
    enriched_contract_open_df = pd.concat(
        [enriched_contract_open_df, filtered_contract_df], ignore_index=True)
enriched_contract_open_df = pd.merge(enriched_contract_open_df, expiration_by_contract_df, on=[
    'Symbol'], how='left')
days_to_expiration_series = enriched_contract_open_df.apply(
    lambda row: calculate_dte(
        trading_bar_datetime=row['DateTime'], contract_expiration_datetime=datetime.strptime(
            row['Expiration Date'], '%Y-%m-%d')
    ),
    axis=1
)
enriched_contract_open_df['DTE'] = days_to_expiration_series
enriched_contract_open_df.to_csv(TARGET_FILE_DEST, index=False)

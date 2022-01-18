import enum
import os
import numpy as np
import pandas as pd
import datetime
from typing import List, Tuple
from decimal import Decimal, ROUND_HALF_UP
from pandas.core.frame import DataFrame
from pandas.core.series import Series
from tqdm import trange
import logging
import sys
import getopt

CURRENT_DIR = os.path.dirname(__file__)
RAW_DATA_DIR = os.path.join(
    CURRENT_DIR, '../../data/raw/nasdaq_srf_futures_settlement')
PROCESSED_DATA_DIR = os.path.join(
    CURRENT_DIR, '../../data/processed/futures_contracts/')
LIVE_CATTLE_INTRADAY_TRUE_OPEN_FILE_PATH = os.path.join(
    CURRENT_DIR, '../../data/processed/futures_contracts/contract_open_enriched_true_open.csv'
)
LIVE_CATTLE_INTRADAY_SLIDING_OPEN_FILE_PATH = os.path.join(
    CURRENT_DIR, '../../data/processed/futures_contracts/contract_open_enriched_sliding_open.csv'
)
TRUE_OPEN_TARGET_FILEPATH = os.path.join(
    PROCESSED_DATA_DIR, 'overnight_changes_from_settlement_true_open.csv'
)
SLIDING_OPEN_TARGET_FILEPATH = os.path.join(
    PROCESSED_DATA_DIR, 'overnight_changes_from_settlement_sliding_open.csv'
)
UNIQUE_TRADING_DAYS_LE_CONTRACTS_FILE_PATH = os.path.join(
    PROCESSED_DATA_DIR, 'unique_trading_days_le_contracts.csv'
)

logging.basicConfig(
    format='%(asctime)s - %(levelname)s:%(message)s', level=logging.DEBUG)


class Settlement_Comparison_Interval(enum.Enum):
    '''
    Represents the interval of time between the open price and the settlement price we are comparing it with
    Overnight - Compare a days open price with the prior days settlement price
    Weekly - Compare a days open price with a settlement price from 7 days ago.
    Monthly - Compare a days open price with a settlement price from 4 weeks ago
    Annualy - Compare a days open price with a settlement price from a year ago
    '''
    OVERNIGHT = 'overnight'
    WEEKLY = 'weekly'
    MONTHLY = 'monthly'
    ANNUALY = 'annualy'


def settlement_csv_files_to_analyze(data_dir):
    # Get a list of all the csv files to process
    csv_files = []
    for file in os.listdir(RAW_DATA_DIR):
        csv_files.append(file)
    csv_files.sort()
    return csv_files


def convert_unique_trading_days(filepath) -> List[datetime.date]:
    csv_as_df = pd.read_csv(filepath, parse_dates=[
                            'DateTime'], usecols=['DateTime'])
    return sorted(csv_as_df['DateTime'].dt.date)


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


def round_to_nearest_thousandth(np_float: np.float64) -> Decimal:
    rounded = Decimal(np_float).quantize(
        Decimal('0.001'), rounding=ROUND_HALF_UP)
    return rounded


def contract_month_and_year_from_file_name(filename: str) -> Tuple[str]:
    settlement_data_month_and_year = filename[-9:-4]
    settlement_data_contract_month = settlement_data_month_and_year[0]
    settlement_data_contract_year = settlement_data_month_and_year[1:]
    return settlement_data_contract_month + settlement_data_contract_year[-2:]


def calculate_change_between_open_and_prior_settlement(a_days_open_bar, a_prior_days_settlement_bar):
    # we are missing an open price or prior day settlement price
    if a_days_open_bar is None or a_prior_days_settlement_bar is None:
        return None
    price_change = a_days_open_bar['Open'] - \
        a_prior_days_settlement_bar['Settle']
    return round_to_nearest_thousandth(price_change)


def get_first_bar_available_for_day(a_date: datetime.date, a_contracts_open_data: pd.DataFrame) -> pd.Series:
    minute_bars_for_day = a_contracts_open_data[a_contracts_open_data['DateTime'].dt.date == a_date]
    minute_bars_for_day_sorted = minute_bars_for_day.sort_values(
        by="Open Minutes Offset", ascending=True).reset_index()
    first_avail_bar = minute_bars_for_day_sorted.iloc[0]
    return first_avail_bar


def get_settlement_data_for_previous_trading_day(
    a_date: datetime.date,
    settlement_data_df: pd.DataFrame,
    unique_trading_days: List[datetime.date]
) -> pd.Series:
    index_of_a_date = unique_trading_days.index(a_date)
    try:
        prior_trading_day_date = unique_trading_days[index_of_a_date - 1]
    except ValueError:  # There is no prior trading day we're aware of
        return None
    data_for_previous_trading_day = settlement_data_df[settlement_data_df['Date'].dt.date ==
                                                       prior_trading_day_date]
    number_of_rows = len(data_for_previous_trading_day.index)
    if number_of_rows == 0:
        return (None, 0)
    if number_of_rows > 1:  # Should never happen
        raise Exception('More than one row of settlement data matched')
    num_lookback_days = (a_date - prior_trading_day_date).days
    logging.info(
        f"a_date={a_date} prior_trading_day_date={prior_trading_day_date} total_num_lookback_days={num_lookback_days}")
    return (data_for_previous_trading_day.iloc[0], num_lookback_days)


def get_settlement_data_for_day_in_previous_week(
    a_date: datetime.date,
    settlement_data_df: pd.DataFrame,
    base_lookback_days: datetime.timedelta
) -> pd.Series:
    unique_trading_days = np.sort(settlement_data_df['Date'].dt.date.values)
    first_trading_day_available = unique_trading_days[0]
    additional_days_lookback = datetime.timedelta(days=1)
    lookback_date = a_date - base_lookback_days
    # Check if the date is a trading day and if so get the index
    if lookback_date in unique_trading_days:
        prior_trading_day_date = lookback_date
    # The date is not a trading day so we loop continuously backwards in time in 1 day incremenets to find the closest trading day before it
    else:
        while (lookback_date not in unique_trading_days):
            lookback_date = lookback_date - additional_days_lookback
            if lookback_date < first_trading_day_available:  # A check to make sure we dont enter an infinite loop
                return (None, 0)
            # We found the closest trading day prior to a week ago
            if (lookback_date in unique_trading_days):
                prior_trading_day_date = lookback_date
                break
            additional_days_lookback = additional_days_lookback + \
                datetime.timedelta(days=1)
    data_for_previous_trading_day = settlement_data_df[settlement_data_df['Date'].dt.date ==
                                                       prior_trading_day_date]
    num_lookback_days = (a_date - prior_trading_day_date).days
    logging.debug(
        f"a_date={a_date} prior_trading_day_date={prior_trading_day_date} total_num_lookback_days={num_lookback_days}")
    return (data_for_previous_trading_day.iloc[0], num_lookback_days)


def get_settlement_data_offset_by_interval(
    a_date: datetime.date,
    settlement_data_df: pd.DataFrame,
    offset_interval: Settlement_Comparison_Interval,
    unique_trading_days: List[datetime.date]
):
    match offset_interval.name:
        case Settlement_Comparison_Interval.OVERNIGHT.name:
            return get_settlement_data_for_previous_trading_day(
                a_date=a_date,
                settlement_data_df=settlement_data_df,
                unique_trading_days=unique_trading_days
            )
        case Settlement_Comparison_Interval.WEEKLY.name:
            return get_settlement_data_for_day_in_previous_week(
                a_date=a_date,
                settlement_data_df=settlement_data_df,
                base_lookback_days=datetime.timedelta(weeks=1)
            )
        case Settlement_Comparison_Interval.MONTHLY.name:
            return get_settlement_data_for_day_in_previous_week(
                a_date=a_date,
                settlement_data_df=settlement_data_df,
                base_lookback_days=datetime.timedelta(months=1)
            )
        case _:
            raise Exception('Unsupported Settlement_Comparison_Interval type')


def process_overnight_settlement_changes(
    settlement_csv_filenames: List[str],
    intraday_open_df: pd.DataFrame,
    settlement_comparison_report_interval: Settlement_Comparison_Interval,
    unique_trading_days: List[datetime.date]
):
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
            settlement_bar, num_lookback_days = get_settlement_data_offset_by_interval(
                a_date=a_date,
                settlement_data_df=settlement_data_df,
                offset_interval=settlement_comparison_report_interval,
                unique_trading_days=unique_trading_days
            )
            difference_between_open_price_and_prior_settlement = calculate_change_between_open_and_prior_settlement(
                a_days_open_bar=first_available_bar, a_prior_days_settlement_bar=settlement_bar)
            overnight_settlement_price_changes_df = overnight_settlement_price_changes_df.append(
                {'Symbol': 'LE' + contract_month_and_year, 'Date': a_date, 'Price Difference b/w Open And Prior Day Settlement': difference_between_open_price_and_prior_settlement}, ignore_index=True)
    return overnight_settlement_price_changes_df


# Parse our arguments from the command line to determine the strategy
try:
    opts, args = getopt.getopt(sys.argv[1:], ':i:')
except getopt.GetoptError:
    print('Run this script as follows: create_price_changes_for_settlements.py.py -i <settlement_comparison_report_interval>')
    sys.exit(2)
for opt, arg in opts:
    if opt in ('-i'):
        settlement_comparison_report_interval = Settlement_Comparison_Interval(
            arg)

settlement_csv_filenames = settlement_csv_files_to_analyze(RAW_DATA_DIR)
unique_trading_days = convert_unique_trading_days(
    UNIQUE_TRADING_DAYS_LE_CONTRACTS_FILE_PATH)
logging.info(f"Parsing intraday true open into dataframe")
intraday_true_open_df = intraday_open_to_df(
    LIVE_CATTLE_INTRADAY_TRUE_OPEN_FILE_PATH)
logging.info(f"Parsing intraday sliding open into dataframe")
intraday_sliding_open_df = intraday_open_to_df(
    LIVE_CATTLE_INTRADAY_SLIDING_OPEN_FILE_PATH)
# Overnight Interval
overnight_changes_true_open_df = process_overnight_settlement_changes(
    settlement_csv_filenames=settlement_csv_filenames,
    intraday_open_df=intraday_true_open_df,
    settlement_comparison_report_interval=settlement_comparison_report_interval,
    unique_trading_days=unique_trading_days
)
overnight_changes_sliding_open_df = process_overnight_settlement_changes(
    settlement_csv_filenames=settlement_csv_filenames,
    intraday_open_df=intraday_sliding_open_df,
    settlement_comparison_report_interval=settlement_comparison_report_interval,
    unique_trading_days=unique_trading_days
)
logging.info(f"Saving true open target csv to {TRUE_OPEN_TARGET_FILEPATH}")
overnight_changes_true_open_df.to_csv(TRUE_OPEN_TARGET_FILEPATH, index=False)
logging.info(
    f"Saving sliding open target csv to {SLIDING_OPEN_TARGET_FILEPATH}")
overnight_changes_sliding_open_df.to_csv(
    SLIDING_OPEN_TARGET_FILEPATH, index=False)
logging.info('Done')

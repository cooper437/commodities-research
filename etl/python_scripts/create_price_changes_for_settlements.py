import enum
import os
import numpy as np
import pandas as pd
import datetime
from typing import List, Tuple
from decimal import Decimal, ROUND_HALF_UP
from tqdm import trange
import logging
import sys
import getopt

'''
Analyze our futures contract settlement data and compare it with our intraday open contract data for both true open and sliding open.
Use this analysis to build out a new set of tables that show the changes between the open price of each day and the settlement in the past.
The settlement price for comparison depends on whether you run the script using an overnight, weekly, monthly, or annualy comparison interval.

To change the Settlement_Comparison_Interval that the script uses you can pass in an argument as follows:
create_price_changes_for_settlements.py -i overnight
'''

CURRENT_DIR = os.path.dirname(__file__)
RAW_DATA_DIR = os.path.join(
    CURRENT_DIR, '../../data/raw/nasdaq_srf_futures_settlement')
PROCESSED_DATA_DIR = os.path.join(
    CURRENT_DIR, '../../data/processed/futures_contracts')
SETTLEMENT_DATA_DIR = os.path.join(PROCESSED_DATA_DIR, 'settlement_analytics')
LIVE_CATTLE_INTRADAY_TRUE_OPEN_FILE_PATH = os.path.join(
    CURRENT_DIR, '../../data/processed/futures_contracts/contract_open_enriched_true_open.csv'
)
LIVE_CATTLE_INTRADAY_SLIDING_OPEN_FILE_PATH = os.path.join(
    CURRENT_DIR, '../../data/processed/futures_contracts/contract_open_enriched_sliding_open.csv'
)
TRUE_OPEN_TARGET_FILENAME = 'changes_from_settlement_true_open'
SLIDING_OPEN_TARGET_FILENAME = 'changes_from_settlement_sliding_open'
UNIQUE_TRADING_DAYS_LE_CONTRACTS_FILE_PATH = os.path.join(
    PROCESSED_DATA_DIR, 'unique_trading_days_le_contracts.csv'
)

logging.basicConfig(
    format='%(asctime)s - %(levelname)s:%(message)s', level=logging.INFO)


class Settlement_Comparison_Interval(enum.Enum):
    '''
    Represents the interval of time between the open price and the settlement price we are comparing it with
    Overnight - Compare a days open price with the prior trading days settlement price
    Weekly - Compare a days open price with a settlement price from 7 days ago.
    Monthly - Compare a days open price with a settlement price from 30 days ago
    Annualy - Compare a days open price with the average settlement price of the same month one year ago
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
    '''
    Get the settlement data for the previous trading day. If it's not available then return None.
    '''
    index_of_a_date = unique_trading_days.index(a_date)
    try:
        prior_trading_day_date = unique_trading_days[index_of_a_date - 1]
    except ValueError:  # There is no prior trading day we're aware of
        return None
    data_for_previous_trading_day = settlement_data_df[settlement_data_df['Date'].dt.date ==
                                                       prior_trading_day_date]
    number_of_rows = len(data_for_previous_trading_day.index)
    if number_of_rows == 0:
        return (None, None)
    if number_of_rows > 1:  # Should never happen
        raise Exception('More than one row of settlement data matched')
    num_lookback_days = (a_date - prior_trading_day_date).days
    logging.debug(
        f"a_date={a_date} prior_trading_day_date={prior_trading_day_date} total_num_lookback_days={num_lookback_days}")
    return (data_for_previous_trading_day.iloc[0], num_lookback_days)


def get_settlement_data_for_lookback_days(
    a_date: datetime.date,
    settlement_data_df: pd.DataFrame,
    base_lookback_days: datetime.timedelta
) -> pd.Series:
    '''
    Get the settlement data for a day that is at least base_lookback_days in the past. If that's not available then
    we go backwards in time in 1 day increments until either A) we find a day where we have settlement data available,
    in which case we return it for that day or B) we are before the first_trading_day_available in the settlement dataset in which
    case we return None.
    '''
    unique_trading_days = np.sort(settlement_data_df['Date'].dt.date.values)
    first_trading_day_available = unique_trading_days[0]
    lookback_date = a_date - base_lookback_days
    # Check if the date is a trading day and if so get the index
    if lookback_date in unique_trading_days:
        prior_trading_day_date = lookback_date
    # The date is not a trading day so we loop continuously backwards in time in 1 day incremenets to find the closest trading day before it
    else:
        # Initialize to one additional day in the past
        additional_days_lookback = datetime.timedelta(days=1)
        while (lookback_date not in unique_trading_days):
            lookback_date = lookback_date - additional_days_lookback
            if lookback_date < first_trading_day_available:  # A check to make sure we dont enter an infinite loop
                return (None, None)
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


def get_settlement_data_for_avg_month(
    a_date: datetime.date,
    settlements_by_month_df: pd.DataFrame
) -> pd.Series:
    '''
    Get the settlement data for the same month of the prior year if available. Note that value for this settlement price
    has already been averaged (mean) in an earlier step in the script which is unlike the other report intervals.
    '''
    month = a_date.month
    previous_year = a_date.year - 1
    settlement_for_same_month_last_year_df = settlements_by_month_df[(
        settlements_by_month_df['Year'] == previous_year) & (settlements_by_month_df['Month'] == month)]
    number_of_rows = len(settlement_for_same_month_last_year_df.index)
    if number_of_rows == 0:
        return (None, None)
    if number_of_rows > 1:  # Should never happen
        raise Exception('More than one row of settlement data matched')
    settlement_for_same_month_last_year_series = settlement_for_same_month_last_year_df.iloc[0]
    logging.debug(
        f"a_date={a_date} month={month} previous_year={previous_year}"
    )
    return (settlement_for_same_month_last_year_series, 365)


def get_settlement_data_offset_by_interval(
    a_date: datetime.date,
    settlement_data_df: pd.DataFrame,
    offset_interval: Settlement_Comparison_Interval,
    unique_trading_days: List[datetime.date],
    settlements_by_month_df: pd.DataFrame
):
    match offset_interval.name:
        case Settlement_Comparison_Interval.OVERNIGHT.name:
            return get_settlement_data_for_previous_trading_day(
                a_date=a_date,
                settlement_data_df=settlement_data_df,
                unique_trading_days=unique_trading_days
            )
        case Settlement_Comparison_Interval.WEEKLY.name:
            return get_settlement_data_for_lookback_days(
                a_date=a_date,
                settlement_data_df=settlement_data_df,
                base_lookback_days=datetime.timedelta(days=7)
            )
        case Settlement_Comparison_Interval.MONTHLY.name:
            return get_settlement_data_for_lookback_days(
                a_date=a_date,
                settlement_data_df=settlement_data_df,
                base_lookback_days=datetime.timedelta(days=30)
            )
        case Settlement_Comparison_Interval.ANNUALY.name:
            return get_settlement_data_for_avg_month(
                a_date=a_date,
                settlements_by_month_df=settlements_by_month_df
            )
        case _:
            raise Exception('Unsupported Settlement_Comparison_Interval type')


def group_settlement_data_by_month(settlement_data_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Group the settlement data by month and take the mean value for each month
    '''
    settlement_data_limited_df = settlement_data_df[['Date', 'Settle']].copy()
    settlement_data_limited_df = settlement_data_limited_df.set_index('Date')
    settlement_mean_by_month_df = settlement_data_limited_df.resample(
        'M').mean()
    settlement_mean_by_month_df['Year'] = settlement_mean_by_month_df.index.year
    settlement_mean_by_month_df['Month'] = settlement_mean_by_month_df.index.month
    return settlement_mean_by_month_df


def process_settlement_changes(
    settlement_csv_filenames: List[str],
    intraday_open_df: pd.DataFrame,
    settlement_comparison_report_interval: Settlement_Comparison_Interval,
    unique_trading_days: List[datetime.date]
) -> pd.DataFrame:
    '''
    Process each contract and then each day within each contract calculcating the difference between the open price for that day
    and the settlement price at some point in the past (day, week, month, year). Return a dataframe that containes the price difference
    for that day.
    '''
    overnight_settlement_price_changes_df = pd.DataFrame(
        columns=['Date', 'Symbol', 'Price Difference b/w Open And Prior Day Settlement'])
    # Process each table of settlement data - one for each contract
    for item_index in trange(len(settlement_csv_filenames)):
        filename = settlement_csv_filenames[item_index]
        contract_month_and_year = contract_month_and_year_from_file_name(
            filename)
        settlement_data_df = settlement_data_to_df(filename)
        settlements_by_month_df = group_settlement_data_by_month(
            settlement_data_df)
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
                unique_trading_days=unique_trading_days,
                settlements_by_month_df=settlements_by_month_df
            )
            difference_between_open_price_and_prior_settlement = calculate_change_between_open_and_prior_settlement(
                a_days_open_bar=first_available_bar, a_prior_days_settlement_bar=settlement_bar)
            overnight_settlement_price_changes_df = overnight_settlement_price_changes_df.append(
                {
                    'Symbol': 'LE' + contract_month_and_year,
                    'Date': a_date,
                    'Price Difference b/w Open And Prior Day Settlement': difference_between_open_price_and_prior_settlement,
                    'Days Looking Back': num_lookback_days
                }, ignore_index=True)
    return overnight_settlement_price_changes_df


# Parse our arguments from the command line to determine the settlement comparison report interval
try:
    opts, args = getopt.getopt(sys.argv[1:], ':i:')
except getopt.GetoptError:
    print('Run this script as follows: create_price_changes_for_settlements.py -i <settlement_comparison_report_interval>')
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
overnight_changes_true_open_df = process_settlement_changes(
    settlement_csv_filenames=settlement_csv_filenames,
    intraday_open_df=intraday_true_open_df,
    settlement_comparison_report_interval=settlement_comparison_report_interval,
    unique_trading_days=unique_trading_days
)
overnight_changes_sliding_open_df = process_settlement_changes(
    settlement_csv_filenames=settlement_csv_filenames,
    intraday_open_df=intraday_sliding_open_df,
    settlement_comparison_report_interval=settlement_comparison_report_interval,
    unique_trading_days=unique_trading_days
)
true_open_filepath = os.path.join(
    SETTLEMENT_DATA_DIR, TRUE_OPEN_TARGET_FILENAME + '_' +
    settlement_comparison_report_interval.value + '.csv'
)
sliding_open_filepath = os.path.join(
    SETTLEMENT_DATA_DIR, SLIDING_OPEN_TARGET_FILENAME + '_' +
    settlement_comparison_report_interval.value + '.csv'
)
logging.info(f"Saving true open target csv to {true_open_filepath}")
overnight_changes_true_open_df.to_csv(true_open_filepath, index=False)
logging.info(
    f"Saving sliding open target csv to {sliding_open_filepath}")
overnight_changes_sliding_open_df.to_csv(
    sliding_open_filepath, index=False)
logging.info('Done')

'''
Iterate over our futures contracts dataset from First Rate Data.
Build out a csv table containing all the unique trading days deduped
'''
import os
import pandas as pd
from tqdm import trange

CONTRACTS_PREFIX_MATCHER = 'LE'  # Optional limit if desired
CURRENT_DIR = os.path.dirname(__file__)
RAW_DATA_DIR = os.path.join(
    CURRENT_DIR, '../../data/raw/firstratedata_futures')
PROCESSED_DATA_DIR = os.path.join(
    CURRENT_DIR, '../../data/processed/futures_contracts')
TARGET_FILENAME = 'unique_trading_days_le_contracts.csv'
TARGET_FILE_DEST = os.path.join(PROCESSED_DATA_DIR, TARGET_FILENAME)


def convert_contract_csv_to_df(filename):
    csv_as_df = pd.read_csv(
        f"{RAW_DATA_DIR}/{filename}",
        parse_dates=['DateTime'], usecols=['DateTime']
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


unique_trading_days_df = pd.DataFrame(columns=['DateTime'])
csv_files = csv_files_to_analyze(
    data_dir=RAW_DATA_DIR, filename_prefix_matcher=CONTRACTS_PREFIX_MATCHER)
for item in trange(len(csv_files)):
    file = csv_files[item]
    contract_df = convert_contract_csv_to_df(file)
    dates_only_as_string = contract_df['DateTime'].apply(
        lambda row: row.strftime("%Y-%m-%d"))
    dates_only_as_string_deduped = dates_only_as_string.drop_duplicates()
    unique_trading_days_df = unique_trading_days_df.append(
        dates_only_as_string_deduped.to_frame(), ignore_index=True)
unique_trading_days_df = unique_trading_days_df.drop_duplicates()
unique_trading_days_df = unique_trading_days_df.sort_values(by='DateTime')
unique_trading_days_df.to_csv(TARGET_FILE_DEST, index=False)

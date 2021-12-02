'''
Transform our futures contracts dataset from First Rate Data into a csv table
that contains each contract and its respective expiration date. Write the csv to the
filesystem
'''
import os
import pandas as pd
import time
from tqdm import trange

CONTRACTS_PREFIX_MATCHER = ''  # Optional limit if desired
CURRENT_DIR = os.path.dirname(__file__)
RAW_DATA_DIR = os.path.join(
    CURRENT_DIR, '../../data/raw/firstratedata_futures')
PROCESSED_DATA_DIR = os.path.join(
    CURRENT_DIR, '../../data/processed/futures_contracts')
TARGET_FILENAME = 'expiration_date_by_contract.csv'
TARGET_FILE_DEST = os.path.join(PROCESSED_DATA_DIR, TARGET_FILENAME)


def convert_csv_to_df(filename):
    csv_as_df = pd.read_csv(
        f"{RAW_DATA_DIR}/{filename}",
        parse_dates=['DateTime'], usecols=['DateTime'], index_col=['DateTime']
    )
    return csv_as_df


def get_most_recent_datetime(a_df: pd.DataFrame):
    return a_df.iloc[-1].name.to_pydatetime().date()


def build_target_df(csv_filenames):
    initial_df = pd.DataFrame(columns=['Symbol', 'Expiration Date'])
    for item in trange(len(csv_files)):
        file = csv_files[item]
        contract_symbol = file[0:-4]
        as_df = convert_csv_to_df(file)
        contract_expiration = get_most_recent_datetime(as_df)
        initial_df = initial_df.append(
            {'Symbol': contract_symbol, 'Expiration Date': contract_expiration}, ignore_index=True)
    return initial_df


def csv_files_to_analyze(data_dir, filename_prefix_matcher):
    # Get a list of all the csv files to process
    csv_files = []
    for file in os.listdir(RAW_DATA_DIR):
        if file.startswith(CONTRACTS_PREFIX_MATCHER):
            csv_files.append(file)
    csv_files.sort()
    return csv_files


target_file_exists = os.path.exists(TARGET_FILE_DEST)
if target_file_exists:
    print('The target file already exists and will be overwritten. Abort now to cancel.')
    time.sleep(5)
csv_files = csv_files_to_analyze(
    data_dir=RAW_DATA_DIR, filename_prefix_matcher=CONTRACTS_PREFIX_MATCHER)
print(f"Analyzing {len(csv_files)} the files")
target_df = build_target_df(csv_files)
target_df.to_csv(TARGET_FILE_DEST, index=False)

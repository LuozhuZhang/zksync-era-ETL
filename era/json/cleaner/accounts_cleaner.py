import json
import os
import sys
import pickle
from collections import defaultdict

from era.json.resolver.account_resolver import EraAccountResolver

from era.utils.data import hex_to_dec, get_block_timestamp
from era.utils.json_to_csv import json_to_csv
from era.json.structures.account import EraAccountType
from era.setup.config import FILE_SIZE, FOLDER_SIZE, START_BLOCK, END_BLOCK

default_keys = ['type','account_address','transaction_hash','block_time','block_number','block_hash','nonce']

# Unique accounts. Create directory if it doesn't exist
directory_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..', '..', 'data', 'unique_accounts', 'era')
os.makedirs(directory_path, exist_ok=True)
input_file_path = os.path.join(directory_path, 'unique_accounts_list.pkl')

class AccountsETL:
  def __init__(self):
    self.blocks_base_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..', 'data', 'json_raw_data', 'era', 'blocks')
    self.clean_base_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..', 'data', 'json_clean_data', 'era', 'all_accounts')
    self.csv_base_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..', 'data', 'json_to_csv', 'era', 'all_accounts')
    self.account_resolver = EraAccountResolver()
    try:
      with open(input_file_path, 'rb') as f:
        self.unique_accounts_set = pickle.load(f)
    except FileNotFoundError:
      self.unique_accounts_set = set()

  def process_accounts(self, blocks_data, output_json_folder, output_csv_folder, file_start, file_end):
    first_account_occurrences = defaultdict(EraAccountType)
    last_account_nonces = defaultdict(int)
    all_accounts = []

    for block_raw_data in blocks_data:
      if 'result' not in block_raw_data or 'transactions' not in block_raw_data['result']:
        print(f"Skipping block data: {block_raw_data}, 'result' or 'transactions' key not found.")
        continue
  
      for transaction_raw_data in block_raw_data['result']['transactions']:
        account_address = transaction_raw_data.get('from')
        block_number = hex_to_dec(transaction_raw_data.get('blockNumber'))

        # Create a hashable account identifier
        account_identifier = (account_address)

        if account_identifier in self.unique_accounts_set:
          continue

        nonce = hex_to_dec(transaction_raw_data.get('nonce'))
        last_account_nonces[account_address] = nonce

        if account_address not in first_account_occurrences:
          account = EraAccountType()
          account.account_address = account_address
          account.transaction_hash = transaction_raw_data.get('hash')
          account.block_time = get_block_timestamp(block_number, blocks_data)
          account.block_number = block_number
          account.block_hash = transaction_raw_data.get('blockHash')

          first_account_occurrences[account_address] = account

          self.unique_accounts_set.add(account_identifier)

    for account_address, account in first_account_occurrences.items():
      account.nonce = last_account_nonces[account_address]
      account_dict = self.account_resolver.account_to_dict(account)
      all_accounts.append(account_dict)
      
    output_json_file_name = f"{file_start}_{file_end}.json"
    print(f"  Accounts - Processing json file: {output_json_file_name}")
    output_json_file_path = os.path.join(output_json_folder, output_json_file_name)
    with open(output_json_file_path, 'w') as file:
      json.dump(all_accounts, file)

    output_csv_file_name = f"{file_start}_{file_end}.csv"
    print(f"  Accounts - Processing csv file: {output_csv_file_name}")
    output_csv_file_path = os.path.join(output_csv_folder, output_csv_file_name)
    json_to_csv(all_accounts, output_csv_file_path, default_keys=default_keys)

  def execute(self):
    for folder_start in range(START_BLOCK, END_BLOCK, FOLDER_SIZE):
      folder_end = folder_start + FOLDER_SIZE
      raw_folder_name = f"blocks_raw_{folder_start}_{folder_end}"
      print(f"Accounts - Processing folder: {raw_folder_name}")

      clean_folder_name = f"all_accounts_{folder_start}_{folder_end}"
      clean_folder_path = os.path.join(self.clean_base_folder, clean_folder_name)
      os.makedirs(clean_folder_path, exist_ok=True)

      csv_folder_name = f"all_accounts_{folder_start}_{folder_end}"
      csv_folder_path = os.path.join(self.csv_base_folder, csv_folder_name)
      os.makedirs(csv_folder_path, exist_ok=True)

      for file_start in range(folder_start, folder_end, FILE_SIZE):
        file_end = file_start + FILE_SIZE
        raw_file_name = f"{file_start}_{file_end}.json"
        raw_file_path = os.path.join(self.blocks_base_folder, raw_folder_name, raw_file_name)
        
        try:
          with open(raw_file_path, 'r') as file:
            blocks_data = json.load(file)
          self.process_accounts(blocks_data, clean_folder_path, csv_folder_path, file_start, file_end)
        except FileNotFoundError:
          print(f"  Accounts - File {raw_file_path} not found.")

    # Saving unique accounts
    output_file_path = os.path.join(directory_path, 'unique_accounts_list.pkl')
    with open(output_file_path, 'wb') as f:
      pickle.dump(self.unique_accounts_set, f)
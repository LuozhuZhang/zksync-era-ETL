import json
import os
import sys

from era.json.resolver.token_transfer_resolver import EraTokenTransferResolver
from era.utils.data import decode_transfer_data
from era.utils.json_to_csv import json_to_csv
from era.setup.config import FILE_SIZE, FOLDER_SIZE, START_BLOCK, END_BLOCK
from era.json.structures.token_transfer import EraTokenTransferType

default_keys = ['type','transaction_hash','block_number','block_time','token_address','from_address','to_address','amount']

class TokenTransfersETL:
  def __init__(self):
    self.transactions_base_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..', 'data', 'json_clean_data', 'era', 'all_transactions')
    self.clean_base_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..', 'data', 'json_clean_data', 'era', 'all_token_transfers')
    self.csv_base_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..', 'data', 'json_to_csv', 'era', 'all_token_transfers')
    self.token_transfer_resolver = EraTokenTransferResolver()

  def process_token_transfers(self, all_transactions_data, output_json_folder, output_csv_folder, file_start, file_end):
    all_token_transfers = []
    unique_token_transfers = set()

    for transaction_dict in all_transactions_data:
      if transaction_dict['input_data'].startswith('0xa9059cbb'):
        token_transfer = EraTokenTransferType()
        token_transfer.transaction_hash = transaction_dict['transaction_hash']
        token_transfer.block_number = transaction_dict['block_number']
        token_transfer.block_time = transaction_dict['block_time']
        token_transfer.token_address = transaction_dict['to_address']
        token_transfer.from_address = transaction_dict['from_address']
        token_transfer.to_address, token_transfer.amount = decode_transfer_data(transaction_dict['input_data'])
        
        token_transfer_dict = self.token_transfer_resolver.token_transfer_to_dict(token_transfer)
        
        # Convert the dictionary to a tuple of sorted items to make it hashable
        token_transfer_tuple = tuple(sorted(token_transfer_dict.items()))

        # Only add unique token transfers
        if token_transfer_tuple not in unique_token_transfers:
          all_token_transfers.append(token_transfer_dict)
          unique_token_transfers.add(token_transfer_tuple)

    output_json_file_name = f"{file_start}_{file_end}.json"
    print(f"  Token Transfers - Processing json file: {output_json_file_name}")
    output_json_file_path = os.path.join(output_json_folder, output_json_file_name)
    with open(output_json_file_path, 'w') as file:
      json.dump(all_token_transfers, file)

    output_csv_file_name = f"{file_start}_{file_end}.csv"
    print(f"  Token Transfers - Processing csv file: {output_csv_file_name}")
    output_csv_file_path = os.path.join(output_csv_folder, output_csv_file_name)
    json_to_csv(all_token_transfers, output_csv_file_path, default_keys=default_keys)

  def execute(self):
    for folder_start in range(START_BLOCK, END_BLOCK, FOLDER_SIZE):
      folder_end = folder_start + FOLDER_SIZE
      raw_folder_name = f"all_transactions_{folder_start}_{folder_end}"  # Corrected here
      print(f"Token Transfers - Processing folder: {raw_folder_name}")

      clean_folder_name = f"all_token_transfers_{folder_start}_{folder_end}"
      clean_folder_path = os.path.join(self.clean_base_folder, clean_folder_name)
      os.makedirs(clean_folder_path, exist_ok=True)

      csv_folder_name = f"all_token_transfers_{folder_start}_{folder_end}"
      csv_folder_path = os.path.join(self.csv_base_folder, csv_folder_name)
      os.makedirs(csv_folder_path, exist_ok=True)

      for file_start in range(folder_start, folder_end, FILE_SIZE):
        file_end = file_start + FILE_SIZE
        raw_file_name = f"{file_start}_{file_end}.json"
        raw_file_path = os.path.join(self.transactions_base_folder, raw_folder_name, raw_file_name)  # Corrected here
        
        try:
          with open(raw_file_path, 'r') as file:
            all_transactions_data = json.load(file)
          self.process_token_transfers(all_transactions_data, clean_folder_path, csv_folder_path, file_start, file_end)
        except FileNotFoundError:
          print(f"  Token Transfers - File {raw_file_path} not found.")

import json
import os
import sys

from era.json.resolver.transaction_resolver import EraTransactionResolver
from era.utils.data import hex_to_dec
from era.utils.json_to_csv import json_to_csv
from era.setup.config import FILE_SIZE, FOLDER_SIZE, START_BLOCK, END_BLOCK

default_keys = ['type','transaction_hash','nonce','block_hash','block_number','block_time','transaction_index','from_address','to_address','value','gas_limit','gas_price','gas_used','input_data','max_fee_per_gas','max_priority_fee_per_gas','status','l1_batch_number','l1_batch_tx_index']

class TransactionsETL:
  def __init__(self):
    self.blocks_base_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..', 'data', 'json_raw_data', 'era', 'blocks')
    self.transactions_base_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..', 'data', 'json_raw_data', 'era', 'transactions')
    self.clean_base_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..', 'data', 'json_clean_data', 'era', 'all_transactions')
    self.csv_base_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..', 'data', 'json_to_csv', 'era', 'all_transactions')
    self.transaction_resolver = EraTransactionResolver()

  def process_transactions(self, blocks_data, transactions_data, output_json_folder, output_csv_folder, file_start, file_end):
    all_transactions = []
    seen_hashes = set()

    for block_raw_data in blocks_data:
      if 'result' not in block_raw_data:
        print(f"Skipping block data: {block_raw_data}, 'result' key not found.")
        continue
  
      block_timestamp = hex_to_dec(block_raw_data['result'].get('timestamp', '0x0'))

      if 'transactions' not in block_raw_data['result']:
        print(f"Skipping block: No transactions found.")
        continue

      for transaction_raw_data in block_raw_data['result']['transactions']:
        transaction = self.transaction_resolver.json_dict_to_transaction(transaction_raw_data, block_timestamp)  # Corrected here
        
        for transaction_data in transactions_data:
          
          if 'result' not in transaction_data:
            print(f"Skipping transaction data: {transaction_data}, 'result' key not found.")
            continue
      
          if transaction_data['result']['transactionHash'] == transaction.transaction_hash:
            # Skip if this transaction has already been processed
            if transaction.transaction_hash in seen_hashes:
              break
            
            transaction.gasUsed = hex_to_dec(transaction_data['result'].get('gasUsed'))
            transaction.status = hex_to_dec(transaction_data['result'].get('status'))
            
            transaction_dict = self.transaction_resolver.transaction_to_dict(transaction)
            all_transactions.append(transaction_dict)
            
            # Add this transaction hash to the set of seen hashes
            seen_hashes.add(transaction.transaction_hash)
            break  # Exit the loop once the transaction is found and processed

    # Save to JSON
    output_json_file_name = f"{file_start}_{file_end}.json"
    print(f"  TXs - Processing json file: {output_json_file_name}")
    output_json_file_path = os.path.join(output_json_folder, output_json_file_name)
    with open(output_json_file_path, 'w') as file:
      json.dump(all_transactions, file)

    # Save to CSV
    output_csv_file_name = f"{file_start}_{file_end}.csv"
    print(f"  TXs - Processing csv file: {output_csv_file_name}")
    output_csv_file_path = os.path.join(output_csv_folder, output_csv_file_name)
    json_to_csv(all_transactions, output_csv_file_path, default_keys=default_keys)

  def execute(self):
    for folder_start in range(START_BLOCK, END_BLOCK, FOLDER_SIZE):
      folder_end = folder_start + FOLDER_SIZE
      raw_blocks_folder_name = f"blocks_raw_{folder_start}_{folder_end}"
      raw_transactions_folder_name = f"transactions_raw_{folder_start}_{folder_end}"
      print(f"TXs - Processing folder: {raw_blocks_folder_name} and {raw_transactions_folder_name}")

      # Clean json data folder
      clean_folder_name = f"all_transactions_{folder_start}_{folder_end}"
      clean_folder_path = os.path.join(self.clean_base_folder, clean_folder_name)
      os.makedirs(clean_folder_path, exist_ok=True)

      # CSV data folder
      csv_folder_name = f"all_transactions_{folder_start}_{folder_end}"
      csv_folder_path = os.path.join(self.csv_base_folder, csv_folder_name)
      os.makedirs(csv_folder_path, exist_ok=True)

      for file_start in range(folder_start, folder_end, FILE_SIZE):
        file_end = file_start + FILE_SIZE
        raw_blocks_file_name = f"{file_start}_{file_end}.json"
        raw_transactions_file_name = f"{file_start}_{file_end}.json"

        raw_blocks_file_path = os.path.join(self.blocks_base_folder, raw_blocks_folder_name, raw_blocks_file_name)
        raw_transactions_file_path = os.path.join(self.transactions_base_folder, raw_transactions_folder_name, raw_transactions_file_name)
        
        try:
          with open(raw_blocks_file_path, 'r') as file:
            blocks_data = json.load(file)
          with open(raw_transactions_file_path, 'r') as file:
            transactions_data = json.load(file)
          self.process_transactions(blocks_data, transactions_data, clean_folder_path, csv_folder_path, file_start, file_end)
        except FileNotFoundError:
          print(f"  TXs - File {raw_blocks_file_path} or {raw_transactions_file_path} not found.")

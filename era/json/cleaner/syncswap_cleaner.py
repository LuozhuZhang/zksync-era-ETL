import json
import os
import sys
from web3 import Web3, HTTPProvider

from era.abi.syncswap_router import SyncSwap_Router_ABI
from era.json.structures.syncswap import EraSyncSwapType
from era.utils.swap import decode_contract_input_data, get_token_info
from era.utils.json_to_csv import json_to_csv

sys.path.append('../setup')
from era.setup.config import FILE_SIZE, FOLDER_SIZE, START_BLOCK, END_BLOCK, RPC_URL

default_keys = ['type', 'transaction_hash','block_number','block_time','from_address','to_address','from_token_address','from_token_amount','to_token_address','to_token_amount']

class SyncSwapETL:
  def __init__(self):
    self.raw_transactions_base_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..', 'data', 'json_raw_data', 'era', 'transactions')
    self.clean_transactions_base_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..', 'data', 'json_clean_data', 'era', 'all_transactions')
    self.clean_base_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..', 'data', 'json_clean_data', 'era', 'all_syncswaps')
    self.csv_base_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..', 'data', 'json_to_csv', 'era', 'all_syncswaps')
    
    w3 = Web3(HTTPProvider(RPC_URL))
    self.SYNC_SWAP_ROUTER_ADDRESS = Web3.to_checksum_address('0x2da10a1e27bf85cedd8ffb1abbe97e53391c0295')
    self.SWAP_FUNCTION_SIGNATURE = '0x2cc4081e'
    self.syncswap_router_contract = w3.eth.contract(address=self.SYNC_SWAP_ROUTER_ADDRESS, abi=SyncSwap_Router_ABI)

  def process_syncswaps(self, all_transactions, raw_transactions, output_json_folder, output_csv_folder, file_start, file_end):
    syncswap_transactions = []
    seen_hashes = set()  # To keep track of unique transaction hashes
    all_transactions_list = [(tx['transaction_hash'], tx) for tx in all_transactions]
    
    for tx in raw_transactions:
      if 'result' not in tx:
        print(f"Skipping transaction data: {tx}, 'result' key not found.")
        continue
  
      tx_result = tx['result']
      if tx_result['status'] == '0x1':
        if tx_result['to'].lower() == '0x2da10a1e27bf85cedd8ffb1abbe97e53391c0295':

          # Skip if this transaction has already been processed
          if tx_result['transactionHash'] in seen_hashes:
            continue

          # Retrieve all transactions with the same hash
          corresponding_txs = [t for h, t in all_transactions_list if h == tx_result['transactionHash']]

          for corresponding_tx in corresponding_txs:
            if corresponding_tx and corresponding_tx['input_data'].startswith('0x2cc4081e'):
              decoded_input = decode_contract_input_data(self.syncswap_router_contract, corresponding_tx['input_data'])
              from_token_address = decoded_input[1]['paths'][0]['tokenIn']

              if from_token_address == '0x0000000000000000000000000000000000000000':
                from_token_address = '0x000000000000000000000000000000000000800a'

              from_token_amount = decoded_input[1]['paths'][0]['amountIn']
              to_token_address, to_token_amount = get_token_info(tx_result['logs'], corresponding_tx['from_address'])
              
              swap_tx = EraSyncSwapType()
              swap_tx.transaction_hash = corresponding_tx['transaction_hash']
              swap_tx.block_number = corresponding_tx['block_number']
              swap_tx.block_time = corresponding_tx['block_time']
              swap_tx.from_address = corresponding_tx['from_address']
              swap_tx.to_address = corresponding_tx['to_address']
              swap_tx.from_token_address = from_token_address.lower()
              swap_tx.from_token_amount = from_token_amount
              swap_tx.to_token_address = to_token_address.lower()
              swap_tx.to_token_amount = to_token_amount
              syncswap_transactions.append(swap_tx)

          # Add this transaction hash to the set of seen hashes
          seen_hashes.add(tx_result['transactionHash'])
        
    # Save to JSON
    output_json_file_name = f"{file_start}_{file_end}.json"
    print(f"  SyncSwaps - Processing json file: {output_json_file_name}")
    output_json_file_path = os.path.join(output_json_folder, output_json_file_name)
    with open(output_json_file_path, 'w') as f:
      json.dump([tx.__dict__ for tx in syncswap_transactions], f)

    # Save to CSV
    output_csv_file_name = f"{file_start}_{file_end}.csv"
    print(f"  SyncSwaps - Processing csv file: {output_csv_file_name}")
    output_csv_file_path = os.path.join(output_csv_folder, output_csv_file_name)
    json_to_csv([tx.__dict__ for tx in syncswap_transactions], output_csv_file_path, default_keys=default_keys)

  def execute(self):
    for folder_start in range(START_BLOCK, END_BLOCK, FOLDER_SIZE):
      folder_end = folder_start + FOLDER_SIZE
      raw_folder_name = f"all_transactions_{folder_start}_{folder_end}"
      raw_transactions_folder_name = f"transactions_raw_{folder_start}_{folder_end}"
      print(f"SyncSwaps - Processing folder: {raw_folder_name} and {raw_transactions_folder_name}")

      clean_folder_name = f"all_syncswaps_{folder_start}_{folder_end}"
      clean_folder_path = os.path.join(self.clean_base_folder, clean_folder_name)
      os.makedirs(clean_folder_path, exist_ok=True)

      csv_folder_name = f"all_syncswaps_{folder_start}_{folder_end}"
      csv_folder_path = os.path.join(self.csv_base_folder, csv_folder_name)
      os.makedirs(csv_folder_path, exist_ok=True)

      for file_start in range(folder_start, folder_end, FILE_SIZE):
        file_end = file_start + FILE_SIZE
        raw_file_name = f"{file_start}_{file_end}.json"
        raw_file_path = os.path.join(self.clean_transactions_base_folder, raw_folder_name, raw_file_name)
        
        raw_transactions_file_name = f"{file_start}_{file_end}.json"
        raw_transactions_file_path = os.path.join(self.raw_transactions_base_folder, raw_transactions_folder_name, raw_transactions_file_name)
        
        try:
          with open(raw_file_path, 'r') as file:
            all_transactions = json.load(file)
          with open(raw_transactions_file_path, 'r') as file:
            raw_transactions = json.load(file)

          self.process_syncswaps(all_transactions, raw_transactions, clean_folder_path, csv_folder_path, file_start, file_end)
        except FileNotFoundError:
          print(f"  SyncSwaps - File {raw_file_path} or {raw_transactions_file_path} not found.")

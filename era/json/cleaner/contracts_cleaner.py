import json
import os
import sys
from collections import defaultdict

from era.json.structures.contract import EraContractType
from era.json.resolver.contract_resolver import EraContractResolver
from era.utils.data import hex_to_dec, get_block_timestamp
from era.utils.json_to_csv import json_to_csv

sys.path.append('../setup')
from era.setup.config import FILE_SIZE, FOLDER_SIZE, START_BLOCK, END_BLOCK

default_keys = ['type','contract_address','transaction_hash','block_time','block_number','block_hash','from_address','to_address']

class ContractsETL:
    def __init__(self):
        self.blocks_base_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..', 'data', 'json_raw_data', 'era', 'blocks')
        self.transactions_base_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..', 'data', 'json_raw_data', 'era', 'transactions')
        self.clean_base_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..', 'data', 'json_clean_data', 'era', 'all_contracts')
        self.csv_base_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..', 'data', 'json_to_csv', 'era', 'all_contracts')
        self.contract_resolver = EraContractResolver()

    def process_unique_contracts(self, blocks_data, transactions_data, output_json_folder, output_csv_folder, file_start, file_end):
        first_contract_occurrences = defaultdict(EraContractType)
        all_contracts = []

        for transaction_raw_data in transactions_data:
          if 'result' not in transaction_raw_data:
            print(f"Skipping transaction data: {transaction_raw_data}, 'result' key not found.")
            continue

          contract_address = transaction_raw_data['result'].get('contractAddress')
          block_number = hex_to_dec(transaction_raw_data['result'].get('blockNumber'))

          # Check if the contract address is not None and if it's the first occurrence
          if contract_address is not None and contract_address not in first_contract_occurrences:
            # Create a new EraContractType instance
            contract = EraContractType()

            # Populate the contract fields
            contract.contract_address = contract_address
            contract.transaction_hash = transaction_raw_data['result'].get('transactionHash')
            contract.block_time = get_block_timestamp(block_number, blocks_data)
            contract.block_number = block_number
            contract.block_hash = transaction_raw_data['result'].get('blockHash')
            contract.from_address = transaction_raw_data['result'].get('from')
            contract.to_address = transaction_raw_data['result'].get('to')

            # Save the first occurrence
            first_contract_occurrences[contract_address] = contract

        # Now that we have the first occurrences, we can convert them to a dict
        for contract in first_contract_occurrences.values():
          contract_dict = self.contract_resolver.contract_to_dict(contract)
          all_contracts.append(contract_dict)
        
        # Save to JSON
        output_json_file_name = f"{file_start}_{file_end}.json"
        print(f"  Contracts - Processing json file: {output_json_file_name}")
        output_json_file_path = os.path.join(output_json_folder, output_json_file_name)
        with open(output_json_file_path, 'w') as file:
            json.dump(all_contracts, file)

        # Save to CSV
        output_csv_file_name = f"{file_start}_{file_end}.csv"
        print(f"  Contracts - Processing csv file: {output_csv_file_name}")
        output_csv_file_path = os.path.join(output_csv_folder, output_csv_file_name)
        json_to_csv(all_contracts, output_csv_file_path, default_keys=default_keys)

    def execute(self):
        for folder_start in range(START_BLOCK, END_BLOCK, FOLDER_SIZE):
            folder_end = folder_start + FOLDER_SIZE
            raw_blocks_folder_name = f"blocks_raw_{folder_start}_{folder_end}"
            raw_transactions_folder_name = f"transactions_raw_{folder_start}_{folder_end}"
            print(f"Contracts - Processing folder: {raw_blocks_folder_name} and {raw_transactions_folder_name}")

            # Clean json data folder
            clean_folder_name = f"all_contracts_{folder_start}_{folder_end}"
            clean_folder_path = os.path.join(self.clean_base_folder, clean_folder_name)
            os.makedirs(clean_folder_path, exist_ok=True)

            # CSV data folder
            csv_folder_name = f"all_contracts_{folder_start}_{folder_end}"
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
                    self.process_unique_contracts(blocks_data, transactions_data, clean_folder_path, csv_folder_path, file_start, file_end)
                except FileNotFoundError:
                    print(f"  Contracts - File {raw_blocks_file_path} or {raw_transactions_file_path} not found.")

# BlockETL.py

import json
import os
import sys

from era.json.resolver.block_resolver import EraBlockResolver
from era.utils.json_to_csv import json_to_csv
from era.setup.config import FILE_SIZE, FOLDER_SIZE, START_BLOCK, END_BLOCK

default_keys = ['type','block_number','block_hash','block_time','parent_hash','sha3_uncles','gas_used','gas_limit','base_fee_per_gas','transaction_count','l1_batch_number','l1_batch_timestamp']

class BlocksETL:
  def __init__(self):
    self.blocks_base_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..', 'data', 'json_raw_data', 'era', 'blocks')
    self.clean_base_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..', 'data', 'json_clean_data', 'era', 'all_blocks')
    self.csv_base_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..', 'data', 'json_to_csv', 'era', 'all_blocks')
    self.block_resolver = EraBlockResolver()

  def process_blocks(self, blocks_data, output_json_folder, output_csv_folder, file_start, file_end):
    all_blocks = []
    seen_hashes = set()  # To keep track of unique block hashes

    for block_raw_data in blocks_data:
      if 'result' in block_raw_data:
        block_hash = block_raw_data['result']['hash']
      
        # Skip if this block has already been processed
        if block_hash in seen_hashes:
          continue

        block = self.block_resolver.json_dict_to_block(block_raw_data['result'])
        block_dict = self.block_resolver.block_to_dict(block)
        all_blocks.append(block_dict)

        # Add this block hash to the set of seen hashes
        seen_hashes.add(block_hash)
      else:
        print(f"Skipping blocks data: {block_raw_data}, 'result' key not found.")
    
    # Save to JSON
    output_json_file_name = f"{file_start}_{file_end}.json"
    print(f"  Blocks - Processing json file: {output_json_file_name}")
    output_json_file_path = os.path.join(output_json_folder, output_json_file_name)
    with open(output_json_file_path, 'w') as file:
      json.dump(all_blocks, file)
    
    # Save to CSV
    output_csv_file_name = f"{file_start}_{file_end}.csv"
    print(f"  Blocks - Processing csv file: {output_csv_file_name}")
    output_csv_file_path = os.path.join(output_csv_folder, output_csv_file_name)
    json_to_csv(all_blocks, output_csv_file_path, default_keys=default_keys)
  
  def execute(self):
    for folder_start in range(START_BLOCK, END_BLOCK, FOLDER_SIZE):  # Update range for full data
      folder_end = folder_start + FOLDER_SIZE
      raw_folder_name = f"blocks_raw_{folder_start}_{folder_end}"
      print(f"Blocks - Processing folder: {raw_folder_name}")

      # clean json data
      clean_folder_name = f"all_blocks_{folder_start}_{folder_end}"
      clean_folder_path = os.path.join(self.clean_base_folder, clean_folder_name)
      os.makedirs(clean_folder_path, exist_ok=True)

      # csv data
      csv_folder_name = f"all_blocks_{folder_start}_{folder_end}"
      csv_folder_path = os.path.join(self.csv_base_folder, csv_folder_name)
      os.makedirs(csv_folder_path, exist_ok=True)

      for file_start in range(folder_start, folder_end, FILE_SIZE):
        file_end = file_start + FILE_SIZE
        raw_file_name = f"{file_start}_{file_end}.json"
        raw_file_path = os.path.join(self.blocks_base_folder, raw_folder_name, raw_file_name)
        
        try:
          with open(raw_file_path, 'r') as file:
            blocks_data = json.load(file)
          self.process_blocks(blocks_data, clean_folder_path, csv_folder_path, file_start, file_end)
        except FileNotFoundError:
          print(f"  Blocks - File {raw_file_path} not found.")

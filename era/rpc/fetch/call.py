import sys
import time
import json
import os
import requests

from era.rpc.fetch.json_rpc_requests import generate_get_block_by_number_json_rpc, generate_get_receipt_json_rpc
from era.utils.files_split import get_folder_and_file_path, write_to_file
from era.setup.config import START_BLOCK, END_BLOCK, BATCH_SIZE

from decouple import config


# Constants
start_block = START_BLOCK
end_block = END_BLOCK
batch_size = BATCH_SIZE
MAX_RETRIES = 100
RETRY_DELAY = 5
url = config('RPC_URL')

def get_batches(lst, batch_size):
  """Yield successive n-sized chunks from lst."""
  for i in range(0, len(lst), batch_size):
    yield lst[i:i + batch_size]

block_numbers = list(range(start_block, end_block))
blocks_base_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..', '..', 'data', 'json_raw_data', 'era', 'blocks')
txs_base_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..', '..', 'data', 'json_raw_data', 'era', 'transactions')

# Iterate over batches of block numbers
for batch in get_batches(block_numbers, batch_size):
  blocks_data = []
  transactions_data = []
  json_rpc_requests = [json.dumps(req) for req in generate_get_block_by_number_json_rpc(batch, True)]

  for request in json_rpc_requests:
    retries = 0
    while retries < MAX_RETRIES:
      try:
        response = requests.post(url, json=json.loads(request))
        response.raise_for_status()  # Raise HTTPError for bad responses
        block_data = json.loads(response.content)
        blocks_data.append(block_data)
        
        for tx in block_data['result']['transactions']:
          tx_hash = tx['hash']
          receipt_request = json.dumps(next(generate_get_receipt_json_rpc([tx_hash])))
          receipt_response = requests.post(url, json=json.loads(receipt_request))
          transaction_data = json.loads(receipt_response.content)
          transactions_data.append(transaction_data)

        print(f"Successfully fetched data for block numbers in {batch}.")
        
        # If successful, break the retry loop
        break
      
      except (requests.RequestException, KeyError, json.JSONDecodeError) as e:
        print(f"An error occurred in {batch}: {e}. Retrying... {retries} times.")
        retries += 1
        time.sleep(RETRY_DELAY)

    if retries == MAX_RETRIES:
      print(f"Failed to fetch data for block numbers in {batch} after {MAX_RETRIES} retries.") 
    
  # Determine which files these blocks should be written to
  first_block_in_batch = batch[0]
  blocks_file_path = get_folder_and_file_path(blocks_base_folder, 'blocks_raw', first_block_in_batch)
  transactions_file_path = get_folder_and_file_path(txs_base_folder, 'transactions_raw', first_block_in_batch)
  
  # Write the blocks and transactions data to their respective files
  write_to_file(blocks_file_path, blocks_data)
  write_to_file(transactions_file_path, transactions_data)
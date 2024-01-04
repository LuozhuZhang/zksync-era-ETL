import requests
import json
import time

from web3 import Web3
from web3.contract import Contract

from era.abi.erc20_abi import ERC20_ABI
from era.setup.config import BALANCE_BATCH_SIZE

MAX_RETRIES = 100
RETRY_DELAY = 5

class ERC20ContractSetup:
  def __init__(self, provider_url: str):
    self.w3 = Web3(Web3.HTTPProvider(provider_url))
    self.contracts = {}

  def initialize_contracts(self, token_address_dict: dict):
    for token_name, token_address in token_address_dict.items():
      if self.w3.is_address(token_address):
        self.contracts[token_name] = self.w3.eth.contract(address=token_address, abi=ERC20_ABI)
      else:
        print(f"Invalid address for {token_name}")

  def retry_request(self, batch, headers):
    retries = 0
    specific_error_retries = 0
    general_retries = 0
    
    MAX_RPC_ERROR_RETRIES = 1000
    MAX_GENERAL_RETRIES = 1000

    while retries < MAX_RETRIES:
      try:
        response = requests.post(self.w3.provider.endpoint_uri, json=batch, headers=headers)
        if response.status_code == 200:
          data = json.loads(response.text)
          
          # Handle specific known error
          if isinstance(data, dict) and 'error' in data and data['error']['message'] == "we can't execute this request":
            specific_error_retries += 1
            time.sleep(2)
            if specific_error_retries < MAX_RPC_ERROR_RETRIES:
              continue
            else:
              return None, f"Failed to get a successful response for the specific error after {MAX_RPC_ERROR_RETRIES} retries."
          
          # Handle unexpected data structure or general errors
          elif not all(isinstance(x, dict) and 'result' in x for x in data):
            general_retries += 1
            time.sleep(2)
            if general_retries < MAX_GENERAL_RETRIES:
              continue
            else:
              return None, f"Failed to get a successful response after {MAX_GENERAL_RETRIES} general retries."
          
          # Successful case
          return data, None
        else:
          raise requests.RequestException(f"Batch request failed with status code {response.status_code}.")
      except (requests.RequestException, requests.ConnectionError) as e:
        print(f"An error occurred: {e}. Retrying... {retries+1} times.")
        retries += 1
        time.sleep(RETRY_DELAY)
    return None, f"Failed to get a successful response after {MAX_RETRIES} retries."

  def get_balances(self, token_name: str, account_addresses: list) -> list:
    if token_name not in self.contracts:
      return [0] * len(account_addresses)
    
    contract: Contract = self.contracts[token_name]
    decoded_balances = [0] * len(account_addresses)  # Initialize with zeros
    
    batch_size = BALANCE_BATCH_SIZE  # Number of requests per batch
    for i in range(0, len(account_addresses), batch_size):
      sub_batch_addresses = account_addresses[i:i + batch_size]
      batch = []
      
      for j, address in enumerate(sub_batch_addresses):
        rpc_method = 'eth_call'
        transaction = {
          'to': contract.address,
          'data': contract.functions.balanceOf(address)._encode_transaction_data()
        }
        rpc_params = [transaction, 'latest']
        batch.append({
          'jsonrpc': '2.0',
          'id': j,
          'method': rpc_method,
          'params': rpc_params
        })

      headers = {'Content-type': 'application/json'}
      results, error = self.retry_request(batch, headers)

      if results is not None:
        for j, result in enumerate(results):
          if isinstance(result, dict):
            if 'result' in result:
              decoded_balances[i+j] = Web3.to_int(hexstr=result['result'])
            elif 'error' in result:
              print(f"Received error for account {sub_batch_addresses[j]}: {result['error']}")
          else:
            print(f"Unexpected result format: {result}")

      else:
        print(error)
        return [0] * len(account_addresses)

    return decoded_balances

  def get_eth_balances(self, account_addresses: list) -> list:
    decoded_balances = [0] * len(account_addresses)  # Initialize with zeros
    
    batch_size = BALANCE_BATCH_SIZE  # Number of requests per batch
    for i in range(0, len(account_addresses), batch_size):
      sub_batch_addresses = account_addresses[i:i + batch_size]
      batch = []
      
      for j, address in enumerate(sub_batch_addresses):
        rpc_method = 'eth_getBalance'
        rpc_params = [address, 'latest']  # Assuming EthBlockParams.LATEST.value resolves to 'latest'
        batch.append({
          'jsonrpc': '2.0',
          'id': j,
          'method': rpc_method,
          'params': rpc_params
        })

      headers = {'Content-type': 'application/json'}
      
      results, error = self.retry_request(batch, headers)

      if results is not None:
        for j, result in enumerate(results):
          if isinstance(result, dict):
            if 'result' in result:
              decoded_balances[i+j] = Web3.to_int(hexstr=result['result'])  # Update the correct index
            elif 'error' in result:
              print(f"Received error for account {sub_batch_addresses[j]}: {result['error']}")
          else:
            print(f"Unexpected result format: {result}")
      else:
        print(error)
        return [0] * len(account_addresses)

    return decoded_balances

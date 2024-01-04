from web3 import Web3
import json
import os

from era.utils.json_to_csv import json_to_csv
from era.utils.erc20_contracts_setup import ERC20ContractSetup
from era.setup.tokens import TOKEN_LIST
from era.setup.config import FILE_SIZE, FOLDER_SIZE, START_BLOCK, END_BLOCK, PRIVATE_RPC

default_keys = [
  'type',
  'account',
  'ETH_address',
  'ETH_balance',
  'WETH_token_address',
  'WETH_token_balance',
  'USDC_token_address',
  'USDC_token_balance',
  'USDT_token_address',
  'USDT_token_balance',
  'BUSD_token_address',
  'BUSD_token_balance'
]

class TokenBalancesETL:
  def __init__(self):
    self.account_base_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..', 'data', 'json_clean_data', 'era', 'all_accounts')
    self.clean_base_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..', 'data', 'json_clean_data', 'era', 'all_balances')
    self.csv_base_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..', 'data', 'json_to_csv', 'era', 'all_balances')
    rpc_urls = [PRIVATE_RPC]
    self.erc20_setups = [ERC20ContractSetup(url) for url in rpc_urls]
    for setup in self.erc20_setups:
      setup.initialize_contracts(TOKEN_LIST)

  def process_token_balances(self, accounts_data, output_json_folder, output_csv_folder, file_start, file_end):
    n = len(accounts_data)
    split_n = n // 4
    accounts_parts = [accounts_data[i:i + split_n] for i in range(0, n, split_n)]

    all_token_balances = []

    if len(self.erc20_setups) == 0:
      print("Error: ERC20 setups not initialized.")
      return

    for i, accounts_part in enumerate(accounts_parts):
      erc20_setup = self.erc20_setups[i % len(self.erc20_setups)]
      token_balances_part = [{'type': 'balance', 'account': Web3.to_checksum_address(data['account_address'])} for data in accounts_part]

      for token_name in TOKEN_LIST.keys():
        account_addresses = [Web3.to_checksum_address(data['account_address']) for data in accounts_part]
        balances = erc20_setup.get_balances(token_name, account_addresses)
        eth_balances = erc20_setup.get_eth_balances(account_addresses)
        # print(f"Balances for {token_name}: {balances}")  # Debugging line

        for j, eth_balance in enumerate(eth_balances):
          if eth_balance > 0:
            token_balances_part[j]['ETH_address'] = '0x000000000000000000000000000000000000800a'
            token_balances_part[j]['ETH_balance'] = eth_balance

        for j, balance in enumerate(balances):
          if balance > 0:
            token_balances_part[j][f'{token_name}_token_address'] = TOKEN_LIST[token_name]
            token_balances_part[j][f'{token_name}_token_balance'] = balance

      all_token_balances.extend(token_balances_part)

    output_json_file_name = f"{file_start}_{file_end}.json"
    print(f"  Balances - Processing json file: {output_json_file_name}")
    output_json_file_path = os.path.join(output_json_folder, output_json_file_name)
    with open(output_json_file_path, 'w') as file:
      json.dump(all_token_balances, file)

    output_csv_file_name = f"{file_start}_{file_end}.csv"
    print(f"  Balances - Processing csv file: {output_csv_file_name}")
    output_csv_file_path = os.path.join(output_csv_folder, output_csv_file_name)
    json_to_csv(all_token_balances, output_csv_file_path, default_keys=default_keys)

  def execute(self):
    for folder_start in range(START_BLOCK, END_BLOCK, FOLDER_SIZE):
      folder_end = folder_start + FOLDER_SIZE
      account_folder_name = f"all_accounts_{folder_start}_{folder_end}"
      account_folder_path = os.path.join(self.account_base_folder, account_folder_name)
      print(f"Balances - Processing folder: {account_folder_name}")
      
      clean_folder_name = f"all_balances_{folder_start}_{folder_end}"
      clean_folder_path = os.path.join(self.clean_base_folder, clean_folder_name)
      os.makedirs(clean_folder_path, exist_ok=True)

      csv_folder_name = f"all_balances_{folder_start}_{folder_end}"
      csv_folder_path = os.path.join(self.csv_base_folder, csv_folder_name)
      os.makedirs(csv_folder_path, exist_ok=True)

      for file_start in range(folder_start, folder_end, FILE_SIZE):
        file_end = file_start + FILE_SIZE
        account_file_name = f"{file_start}_{file_end}.json"
        account_file_path = os.path.join(account_folder_path, account_file_name)
        
        try:
          with open(account_file_path, 'r') as file:
            accounts_data = json.load(file)
          self.process_token_balances(accounts_data, clean_folder_path, csv_folder_path, file_start, file_end)
        except FileNotFoundError:
          print(f"  Balances - File {account_file_path} not found.")

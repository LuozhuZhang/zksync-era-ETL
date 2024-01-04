import sys

from era.json.structures.account import EraAccountType
from era.utils.data import hex_to_dec

class EraAccountResolver(object):
  def json_dict_to_account(self, json_dict):
    account = EraAccountType()
    account.account_address = json_dict.get('address')
    account.transaction_hash = json_dict.get('transactionHash')
    account.block_time = hex_to_dec(json_dict.get('timestamp'))
    account.block_number = hex_to_dec(json_dict.get('blockNumber'))
    account.block_hash = json_dict.get('blockHash')
    account.nonce = hex_to_dec(json_dict.get('nonce'))

    return account

  def account_to_dict(self, account):
    return {
      'type': 'account',
      'account_address': account.account_address,
      'transaction_hash': account.transaction_hash,
      'block_time': account.block_time,
      'block_number': account.block_number,
      'block_hash': account.block_hash,
      'nonce': account.nonce
    }

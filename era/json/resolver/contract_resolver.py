import sys

from era.json.structures.contract import EraContractType
from era.utils.data import hex_to_dec

class EraContractResolver(object):
  def json_dict_to_contract(self, json_dict):
    contract = EraContractType()
    contract.contract_address = json_dict.get('address')
    contract.transaction_hash = json_dict.get('transactionHash')
    contract.block_time = hex_to_dec(json_dict.get('timestamp'))
    contract.block_number = hex_to_dec(json_dict.get('blockNumber'))
    contract.block_hash = json_dict.get('blockHash')
    contract.from_address = json_dict.get('from')
    contract.to_address = json_dict.get('to')

    return contract

  def contract_to_dict(self, contract):
    return {
      'type': 'contract',
      'contract_address': contract.contract_address,
      'transaction_hash': contract.transaction_hash,
      'block_time': contract.block_time,
      'block_number': contract.block_number,
      'block_hash': contract.block_hash,
      'from_address': contract.from_address,
      'to_address': contract.to_address,
    }

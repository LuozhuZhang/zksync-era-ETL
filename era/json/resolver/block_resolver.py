import sys

from era.json.structures.block import EraBlockType
from era.utils.data import hex_to_dec

class EraBlockResolver(object):
  def json_dict_to_block(self, json_dict):
    block = EraBlockType()
    block.block_number = hex_to_dec(json_dict.get('number'))
    block.block_hash = json_dict.get('hash')
    block.block_time = hex_to_dec(json_dict.get('timestamp'))
    block.parent_hash = json_dict.get('parentHash')
    block.sha3_uncles = json_dict.get('sha3Uncles')
    block.gas_used = hex_to_dec(json_dict.get('gasUsed'))
    block.gas_limit = hex_to_dec(json_dict.get('gasLimit'))
    block.base_fee_per_gas = hex_to_dec(json_dict.get('baseFeePerGas'))
    transactions = json_dict.get('transactions', [])
    block.transaction_count = len(transactions)
    block.l1_batch_number = hex_to_dec(json_dict.get('l1BatchNumber'))
    block.l1_batch_timestamp = hex_to_dec(json_dict.get('l1BatchTimestamp'))

    return block

  def block_to_dict(self, block):
    return {
      'type': 'block',
      'block_number': block.block_number,
      'block_hash': block.block_hash,
      'block_time': block.block_time,
      'parent_hash': block.parent_hash,
      'sha3_uncles': block.sha3_uncles,
      'gas_used': block.gas_used,
      'gas_limit': block.gas_limit,
      'base_fee_per_gas': block.base_fee_per_gas,
      'transaction_count': block.transaction_count,
      'l1_batch_number': block.l1_batch_number,
      'l1_batch_timestamp': block.l1_batch_timestamp,
    }

import sys

from era.json.structures.transaction import EraTransactionType
from era.utils.data import hex_to_dec

class EraTransactionResolver(object):
  def json_dict_to_transaction(self, json_dict, block_timestamp):
    transaction = EraTransactionType()
    transaction.transaction_hash = json_dict.get('hash')
    transaction.nonce = hex_to_dec(json_dict.get('nonce'))
    transaction.block_hash = json_dict.get('blockHash')
    transaction.block_number = hex_to_dec(json_dict.get('blockNumber'))
    transaction.block_time = block_timestamp if block_timestamp is not None else hex_to_dec(json_dict.get('timestamp', '0x0'))
    transaction.transaction_index = hex_to_dec(json_dict.get('transactionIndex'))
    transaction.from_address = json_dict.get('from')
    transaction.to_address = json_dict.get('to')
    transaction.value = hex_to_dec(json_dict.get('value'))
    transaction.gas_limit = hex_to_dec(json_dict.get('gas'))
    transaction.gas_price = hex_to_dec(json_dict.get('gasPrice'))
    transaction.gas_used = hex_to_dec(json_dict.get('gasUsed'))
    transaction.input_data = json_dict.get('input')
    transaction.max_fee_per_gas = hex_to_dec(json_dict.get('maxFeePerGas'))
    transaction.max_priority_fee_per_gas = hex_to_dec(json_dict.get('maxPriorityFeePerGas'))
    transaction.status = hex_to_dec(json_dict.get('status'))
    transaction.l1_batch_number = hex_to_dec(json_dict.get('l1BatchNumber'))
    transaction.l1_batch_tx_index = hex_to_dec(json_dict.get('l1BatchTxIndex'))
    return transaction

  def transaction_to_dict(self, transaction):
    return {
      'type': 'transaction',
      'transaction_hash': transaction.transaction_hash,
      'nonce': transaction.nonce,
      'block_hash': transaction.block_hash,
      'block_number': transaction.block_number,
      'block_time': transaction.block_time,
      'transaction_index': transaction.transaction_index,
      'from_address': transaction.from_address,
      'to_address': transaction.to_address,
      'value': transaction.value,
      'gas_limit': transaction.gas_limit,
      'gas_price': transaction.gas_price,
      'gas_used': transaction.gasUsed,
      'input_data': transaction.input_data,
      'max_fee_per_gas': transaction.max_fee_per_gas,
      'max_priority_fee_per_gas': transaction.max_priority_fee_per_gas,
      'status': transaction.status,
      'l1_batch_number': transaction.l1_batch_number,
      'l1_batch_tx_index': transaction.l1_batch_tx_index,
    }

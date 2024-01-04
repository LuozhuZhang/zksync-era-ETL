class EraTokenTransferResolver(object):
  def token_transfer_to_dict(self, token_transfer):
    return {
      'type': 'token_transfer',
      'transaction_hash': token_transfer.transaction_hash,
      'block_number': token_transfer.block_number,
      'block_time': token_transfer.block_time,
      'token_address': token_transfer.token_address,
      'from_address': token_transfer.from_address,
      'to_address': token_transfer.to_address,
      'amount': token_transfer.amount,
      }

class EraTransactionType(object):
  def __init__(self):
    self.transaction_hash = None
    self.nonce = None  # type
    self.block_hash = None
    self.block_number = None
    self.block_time = None
    self.transaction_index = None
    self.from_address = None
    self.to_address = None
    self.value = None
    self.gas_limit = None
    self.gas_price = None
    self.gas_used = None
    self.input_data = None
    self.max_fee_per_gas = None
    self.max_priority_fee_per_gas = None
    self.status = None
    self.l1_batch_number = None
    self.l1_batch_tx_index = None
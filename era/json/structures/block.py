class EraBlockType(object):
  def __init__(self):
    self.block_number = None
    self.block_hash = None
    self.block_time = None
    self.parent_hash = None
    self.sha3_uncles = None
    self.gas_used = None
    self.gas_limit = None
    self.base_fee_per_gas = 0

    self.transactions = []
    self.transaction_count = 0

    self.l1_batch_number = None
    self.l1_batch_timestamp = None
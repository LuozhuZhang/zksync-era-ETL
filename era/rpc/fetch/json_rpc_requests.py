def generate_get_block_by_number_json_rpc(block_numbers, include_transactions):
  for idx, block_number in enumerate(block_numbers):
    yield generate_json_rpc(
      method='eth_getBlockByNumber',
      params=[hex(block_number), include_transactions],
      request_id=idx
    )

def generate_get_receipt_json_rpc(transaction_hashes):
  for idx, transaction_hash in enumerate(transaction_hashes):
    yield generate_json_rpc(
      method='eth_getTransactionReceipt',
      params=[transaction_hash],
      request_id=idx
    )

def generate_trace_block_by_number_json_rpc(block_numbers):
  for block_number in block_numbers:
    yield generate_json_rpc(
      method='debug_traceBlockByNumber',
      params=[hex(block_number)],
      # save block_number in request ID, so later we can identify block number in response
      request_id=block_number,
    )

def generate_json_rpc(method, params, request_id=1):
  return {
    'jsonrpc': '2.0',
    'method': method,
    'params': params,
    'id': request_id,
  }
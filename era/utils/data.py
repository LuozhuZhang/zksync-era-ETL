from eth_abi import decode

# Main
def hex_to_dec(hex_string):
  if hex_string is None:
    return None
  try:
    return int(hex_string, 16)
  except ValueError:
    print("Not a hex string %s" % hex_string)
    return hex_string

# Get block timestamp
def get_block_timestamp(block_number, blocks_data):
  for block_raw_data in blocks_data:

    if 'result' not in block_raw_data or 'number' not in block_raw_data['result']:
      print(f"Skipping block data: {block_raw_data}, 'result' or 'number' key not found.")
      continue

    if hex_to_dec(block_raw_data['result']['number']) == block_number:
      return hex_to_dec(block_raw_data['result']['timestamp'])
    
  return None

#  Get transfer data
def decode_transfer_data(input_data):
    types = ['address', 'uint256']  # address and amount
    input_bytes = bytes.fromhex(input_data[10:])
    decoded = decode(types, input_bytes)
    recipient, amount = decoded
    return recipient, amount
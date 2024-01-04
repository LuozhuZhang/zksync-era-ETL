from typing import List, Dict

from era.utils.data import hex_to_dec

# Decode contract input data via ABI
def decode_contract_input_data(smart_contract, input_data: str) -> Dict:
  decoded_input = smart_contract.decode_function_input(input_data)
  return decoded_input

# Address
def decode_topic_data(data: str) -> str:
  return '0x' + data[26:].lower()

# Return to token address & amount
def get_token_info(logs: list, from_address: str) -> (str, int):
  TRANSFER_EVENT_TOPIC = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'.lower()
  PROXY_ADDRESS = '0x621425a1Ef6abE91058E9712575dcc4258F8d091'.lower()

  to_token_address = None
  to_token_amount = None

  for log in logs:
    if TRANSFER_EVENT_TOPIC in log['topics']:
      topic1 = decode_topic_data(log['topics'][1])
      topic2 = decode_topic_data(log['topics'][2])

      if topic1.lower() == PROXY_ADDRESS.lower() and topic2.lower() == from_address.lower():
        to_token_address = log['address']
        to_token_amount = hex_to_dec(log['data'])

  return to_token_address, to_token_amount
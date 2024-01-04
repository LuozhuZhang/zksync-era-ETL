import sys
import json
import os
import requests
from era.rpc.fetch.json_rpc_requests import generate_trace_block_by_number_json_rpc
from era.setup.config import RPC_URL

# Create an empty list to store the responses
responses = []

# example
block_numbers = list(range(0, 100))  # from block 0 to 99
trace_json_rpc_requests = [json.dumps(req) for req in generate_trace_block_by_number_json_rpc(block_numbers)]

url = RPC_URL

traces_data = []
for response, trace_request in zip(responses, trace_json_rpc_requests):
  trace_response = requests.post(url, json=json.loads(trace_request))
  traces_data.append(json.loads(trace_response.content))

# Write the responses to JSON files
if not os.path.exists('./era/json_raw_data'):
  os.makedirs('./era/json_raw_data')

with open('./era/json_raw_data/traces_raw_data.json', 'w') as f:
  json.dump(traces_data, f)

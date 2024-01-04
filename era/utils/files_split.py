import sys
import json
import os
import requests
import time

sys.path.append('./era/setup')
from era.setup.config import FILE_SIZE, FOLDER_SIZE

# Function to determine folder and file paths
def get_folder_and_file_path(base_folder, prefix, block_num, interval=FILE_SIZE, folder_interval=FOLDER_SIZE):
  folder_start = (block_num // folder_interval) * folder_interval
  folder_end = folder_start + folder_interval
  folder_name = f"{prefix}_{folder_start}_{folder_end}"
  folder_path = os.path.join(base_folder, folder_name)
  
  if not os.path.exists(folder_path):
    os.makedirs(folder_path)
  
  file_start = (block_num // interval) * interval
  file_end = file_start + interval
  file_name = f"{file_start}_{file_end}.json"
  file_path = os.path.join(folder_path, file_name)
  
  return file_path

# Write data
def write_to_file(file_path, data):
  existing_data = []
  if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
    with open(file_path, 'r') as f:
      existing_data = json.load(f)
  
  existing_data.extend(data)
  
  with open(file_path, 'w') as f:
    json.dump(existing_data, f)
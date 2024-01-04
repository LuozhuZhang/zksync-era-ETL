import os
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from era.setup.config import FILE_SIZE, FOLDER_SIZE, START_BLOCK, END_BLOCK

def find_missing_files(base_folder, data_type, max_block):
  missing_files = []
  for start_block in range(START_BLOCK, max_block + 1, FOLDER_SIZE):
    for inner_start_block in range(start_block, min(start_block + FOLDER_SIZE, max_block + 1), FILE_SIZE):
      if inner_start_block == max_block:
        continue
      folder_name = f"{start_block}_{min(start_block + FOLDER_SIZE, max_block)}"
      file_name = f"{inner_start_block}_{min(inner_start_block + FILE_SIZE, max_block)}.csv"
      full_path = os.path.join(base_folder, f"{data_type}_{folder_name}", file_name)
      # print(f"Checking path: {full_path}")  # Debugging line
      if not os.path.exists(full_path):
        missing_files.append(file_name)
  return missing_files

def parallel_import(db_config, base_folder, schema_name, table_name, data_type, max_block, import_csv_to_db):
  missing_files = find_missing_files(base_folder, data_type, max_block)
  if missing_files:
    print(f"Missing files: {missing_files}")
    return

  csv_files = [os.path.join(base_folder, f"{data_type}_{start}_{min(start + FOLDER_SIZE, max_block)}", f"{inner}_{min(inner + FILE_SIZE, max_block)}.csv")
                for start in range(START_BLOCK, max_block + 1, FOLDER_SIZE)
                for inner in range(start, min(start + FOLDER_SIZE, max_block), FILE_SIZE)]

  results = []
  with ThreadPoolExecutor() as executor:
    futures = {executor.submit(import_csv_to_db, db_config, csv_file, schema_name, table_name): csv_file for csv_file in csv_files}
    for future in as_completed(futures):
      try:
        result = future.result()
        results.append(result)
      except Exception as e:
        print(f"Failed to import {futures[future]}: {e}")

  sorted_results = sorted(results, key=lambda x: int(x[0].split('_')[0]))
  for result in sorted_results:
    print(f"Imported {result} into {schema_name}.{table_name}")

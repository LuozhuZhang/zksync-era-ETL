import os
import csv
import psycopg2
from decouple import config
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from era.setup.config import END_BLOCK
from era.utils.dump_csv_to_postgresql import parallel_import

def import_balances_to_db(db_config, csv_path, schema_name, table_name):
  try:
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    cur.execute(f"SET search_path TO {schema_name}")
    full_table_name = f"{schema_name}.{table_name}"
    
    with open(csv_path, 'r') as f:
      reader = csv.reader(f)
      next(reader)  # Skip the header
      for row in reader:
        sanitized_row = [(None if val == '' else val) for val in row]
        cur.execute(f"""
          INSERT INTO {full_table_name} (
              type, account, ETH_address, ETH_balance, WETH_token_address, 
              WETH_token_balance, USDC_token_address, USDC_token_balance, 
              USDT_token_address, USDT_token_balance, BUSD_token_address, BUSD_token_balance
          ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, sanitized_row)
    
    conn.commit()
    cur.close()
    conn.close()
    return (os.path.basename(csv_path), f"Imported {os.path.basename(csv_path)} into {full_table_name}")
  
  except Exception as e:
    return (os.path.basename(csv_path), f"Failed to import {os.path.basename(csv_path)} into {full_table_name}. Error: {e}")

def export_balances():
  db_config = {
    'database': config('DATABASE'),
    'user': config('USER'),
    'password': config('PASSWORD'),
    'host': config('HOST'),
    'port': config('PORT')
  }
  current_file_path = os.path.dirname(os.path.realpath(__file__))
  csv_base_folder = os.path.join(current_file_path, '..', '..', '..', 'data', 'json_to_csv', 'era', 'all_balances')
  parallel_import(db_config, csv_base_folder, 'era', 'balances', 'all_balances', END_BLOCK, import_balances_to_db)

if __name__ == "__main__":
  export_balances()

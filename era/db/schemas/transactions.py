import psycopg2
from psycopg2 import sql
from decouple import config

def create_era_transactions():
  # .env
  DATABASE = config('DATABASE')
  USER = config('USER')
  PASSWORD = config('PASSWORD')
  HOST = config('HOST')
  PORT = config('PORT')

  # Connect to your PostgreSQL database
  conn = psycopg2.connect(
    database=DATABASE,
    user=USER,
    password=PASSWORD,
    host=HOST,
    port=PORT
  )
  cur = conn.cursor()

  # Define the schema name
  schema_name = "era"

  # Check if the schema exists, and create it if it doesn't
  cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {};").format(sql.Identifier(schema_name)))

  # Create table under 'era' schema
  create_table_command = """
  CREATE TABLE IF NOT EXISTS {}.transactions (
    type TEXT,
    block_time TIMESTAMPTZ,
    block_number INT8,
    block_hash BYTEA,
    transaction_hash BYTEA PRIMARY KEY,
    from_address BYTEA,
    to_address BYTEA,
    value NUMERIC,
    gas_limit NUMERIC,
    gas_price NUMERIC,
    gas_used NUMERIC,
    input_data BYTEA,
    transaction_index NUMERIC,
    nonce NUMERIC,
    status BOOLEAN,
    max_fee_per_gas NUMERIC,
    max_priority_fee_per_gas NUMERIC,
    l1_batch_number NUMERIC,
    l1_batch_tx_index NUMERIC
  );
  """.format(schema_name)
  cur.execute(create_table_command)
  conn.commit()

  # Close communication with the database
  cur.close()
  conn.close()

  print("era schema, era.transactions table creation completed.")

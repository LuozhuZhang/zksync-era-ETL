import psycopg2
from psycopg2 import sql
from decouple import config

def create_era_blocks():

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
  CREATE TABLE IF NOT EXISTS {}.blocks (
    type TEXT,
    block_time TIMESTAMPTZ,
    block_number INT8,
    block_hash BYTEA PRIMARY KEY,
    parent_hash BYTEA,
    sha3_uncles BYTEA,
    gas_used NUMERIC,
    gas_limit NUMERIC,
    base_fee_per_gas NUMERIC,
    transaction_count INT,
    l1_batch_number NUMERIC,
    l1_batch_timestamp TIMESTAMPTZ
  );
  """.format(schema_name)
  cur.execute(create_table_command)
  conn.commit()

  # Close communication with the database
  cur.close()
  conn.close()

  print("era schema, era.blocks table creation completed.")

import psycopg2
from psycopg2 import sql
from decouple import config

def create_era_balances():
  DATABASE = config('DATABASE')
  USER = config('USER')
  PASSWORD = config('PASSWORD')
  HOST = config('HOST')
  PORT = config('PORT')

  conn = psycopg2.connect(
    database=DATABASE,
    user=USER,
    password=PASSWORD,
    host=HOST,
    port=PORT
  )
  cur = conn.cursor()

  schema_name = "era"

  cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {};").format(sql.Identifier(schema_name)))

  create_table_command = """
  CREATE TABLE IF NOT EXISTS {}.balances (
    type TEXT,
    account BYTEA PRIMARY KEY,
    ETH_address BYTEA,
    ETH_balance NUMERIC NULL,
    WETH_token_address BYTEA,
    WETH_token_balance NUMERIC NULL,
    USDC_token_address BYTEA,
    USDC_token_balance NUMERIC NULL,
    USDT_token_address BYTEA,
    USDT_token_balance NUMERIC NULL,
    BUSD_token_address BYTEA,
    BUSD_token_balance NUMERIC NULL
  );
  """.format(schema_name)
  cur.execute(create_table_command)
  conn.commit()

  cur.close()
  conn.close()

  print("era schema, era.balances table creation completed.")

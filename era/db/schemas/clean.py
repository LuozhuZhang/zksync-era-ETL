import psycopg2
from psycopg2 import sql
from decouple import config

def clear_and_delete_tables():
  DATABASE = config('DATABASE')
  USER = config('USER')
  PASSWORD = config('PASSWORD')
  HOST = config('HOST')
  PORT = config('PORT')

  conn = psycopg2.connect(
    database=DATABASE, user=USER, password=PASSWORD, host=HOST, port=PORT)
  cur = conn.cursor()

  schema_name = "era"
  tables = ["blocks", "transactions", "token_transfers",
          "accounts", "contracts", "syncswap_swaps", "balances"]

  for table in tables:
    check_table_exists_command = sql.SQL(
      "SELECT to_regclass('{}.{}');").format(sql.Identifier(schema_name), sql.Identifier(table))
    cur.execute(check_table_exists_command)
    exists = cur.fetchone()[0]

    if exists:
      print(f"Table {table}, {schema_name}.{table} exists. Clearing data (Schemas)!!!")
      clear_table_command = sql.SQL(
        "TRUNCATE {}.{};").format(sql.Identifier(schema_name), sql.Identifier(table))
      cur.execute(clear_table_command)
      print(f"Data in table: {table}, {schema_name}.{table} has been cleared (Schemas)!!!")

      print(f"Deleting table {table}, {schema_name}.{table}. (Table Data)")
      drop_table_command = sql.SQL(
        "DROP TABLE {}.{};").format(sql.Identifier(schema_name), sql.Identifier(table))
      cur.execute(drop_table_command)
      print(f"Table {table}, {schema_name}.{table} have been deleted. (Table Data)")
    else:
      print(f"Table {table}, {schema_name}.{table} does not exist. Skipping.")

  conn.commit()
  cur.close()
  conn.close()

  print("Operation completed.")

if __name__ == '__main__':
  clear_and_delete_tables()

# main.py

from era.db.schemas.blocks import create_era_blocks
from era.db.schemas.transactions import create_era_transactions
from era.db.schemas.token_transfers import create_era_token_transfers
from era.db.schemas.syncswap_swaps import create_era_syncswap_swaps

from era.db.schemas.accounts import create_era_accounts
from era.db.schemas.contracts import create_era_contracts

from era.db.schemas.balances import create_era_balances

def create_era_tables():
  create_era_blocks()
  create_era_transactions()
  create_era_token_transfers()
  create_era_syncswap_swaps()

  create_era_accounts()
  create_era_contracts()

  create_era_balances()

if __name__ == "__main__":
  create_era_tables()
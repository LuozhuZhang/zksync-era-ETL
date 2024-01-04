from era.db.exporter.blocks_exporter import export_blocks
from era.db.exporter.transactions_exporter import export_transactions
from era.db.exporter.token_transfers_exporter import export_token_transfers
from era.db.exporter.syncswap_exporter import export_syncswap_swaps

from era.db.exporter.accounts_exporter import export_accounts
from era.db.exporter.contracts_exporter import export_contracts

from era.db.exporter.balances_exporter import export_balances

if __name__ == "__main__":
  export_blocks()
  export_transactions()
  export_token_transfers()
  export_syncswap_swaps()

  export_accounts()
  export_contracts()

  export_balances()
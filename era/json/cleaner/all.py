# main.py

from era.json.cleaner.blocks_cleaner import BlocksETL
from era.json.cleaner.transactions_cleaner import TransactionsETL
from era.json.cleaner.contracts_cleaner import ContractsETL
from era.json.cleaner.accounts_cleaner import AccountsETL
from era.json.cleaner.token_transfers_cleaner import TokenTransfersETL
from era.json.cleaner.syncswap_cleaner import SyncSwapETL
from era.json.cleaner.balances_cleaner import TokenBalancesETL

def clean_all_data():
  blocketl = BlocksETL()
  blocketl.execute()

  txetl = TransactionsETL()
  txetl.execute()

  contractetl = ContractsETL()
  contractetl.execute()

  accountsetl = AccountsETL()
  accountsetl.execute()

  tokentransferetl = TokenTransfersETL()
  tokentransferetl.execute()

  syncswapetl = SyncSwapETL()
  syncswapetl.execute()

  balanceetl = TokenBalancesETL()
  balanceetl.execute()

if __name__ == "__main__":
  clean_all_data()
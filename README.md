# zksync-era-ETL: on-chain data tool

### Introduction

As Ethereum continues to evolve, the role of Layer 2 (L2) solutions like rollups becomes increasingly pivotal. These innovations are crucial in reducing transaction costs on Ethereum, but they also present new challenges, such as fragmented liquidity. In this rapidly changing landscape, leading L2 platforms are gaining prominence, and I anticipate that in the near future, a select few will handle the majority of significant transactions.

In this regard, zkSync stands out as a potential leader. Its continuous optimization positions it alongside other major L2 solutions like Optimism and Arbitrum. Recognizing zkSync's potential to become a 'Super Rollup', I developed zkSync-ETL. This tool is designed for efficient and real-time access to on-chain data, a crucial need for developers and analysts in the Ethereum ecosystem.

zkSync-ETL is an ongoing project, and warmly welcome ideas, feedback, and contributions. We ensure it remains a valuable resource for anyone looking to leverage the power of zkSync in their Ethereum-based applications.

### Architecture

#### High-Level

The zkSync-ETL is structured into two primary components: the `/data` module for data storage, and the `/era` module for specific data processing tasks.

Data Acquisition (`/rpc` Module): This module interfaces with the [zkSync RPC](https://chainlist.org/chain/324), where running a local node is advisable (see [external node documentation](https://github.com/matter-labs/zksync-era/tree/main/docs/guides/external-node) for guidance). It retrieves raw block and transaction data in JSON format.

Data Processing (`/json` Module): Within the json module, raw data undergoes cleaning and processing. This transforms it into comprehensively clean data, currently comprising seven core tables: accounts, balances, blocks, contracts, SyncSwap swaps, token transfers, and transactions. Future updates aim to include data from mainstream DEXs, NFTs, and derivative protocols.

Database Management (`/db` Module): The db module is responsible for creating PostgreSQL tables and data schemas. It imports all data in CSV format into these tables. This setup enables the development of custom data programs akin to Dune, Nansen, and The Graph, utilizing zkSync data. Additionally, these datasets can be instrumental in researching the Ethereum and zkSync ecosystems."

#### Low-Level

### How to use it

### Contribution

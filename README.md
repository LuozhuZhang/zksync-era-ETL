# zksync-era-ETL: on-chain data tool

<div align="center">
  <a href="[https://www.artstation.com/artwork/9mEx8a/](https://zksync.io/)">
    <img alt="zkevm" src="https://github.com/LuozhuZhang/zksync-era-ETL/assets/70309026/31e4d8fb-4c43-42c0-870e-3195bb478a14" >
  </a>
  <p align="center">
    <a href="https://github.com/sindresorhus/awesome">
      <img alt="awesome" src="https://cdn.rawgit.com/sindresorhus/awesome/d7305f38d29fed78fa85652e3a63e154dd8e8829/media/badge.svg">
    </a>
    <a href="https://github.com/LuozhuZhang/zksync-era-ETL/graphs/contributors">
      <img alt="GitHub contributors" src="https://img.shields.io/github/contributors/LuozhuZhang/zksync-era-ETL">
    </a>
    <a href="http://makeapullrequest.com">
      <img alt="pull requests welcome badge" src="https://img.shields.io/badge/PRs-welcome-brightgreen.svg?style=flat">
    </a>
    <a href="https://twitter.com/LuozhuZhang">
      <img alt="Twitter" src="https://img.shields.io/twitter/url/https/twitter.com/LuozhuZhang.svg?style=social&label=Follow%20%40LuozhuZhang">
    </a>
  </p>
</div>

## Introduction

As Ethereum continues to evolve, [the role of Layer 2 (L2) solutions like rollups becomes increasingly pivotal](https://ethereum-magicians.org/t/a-rollup-centric-ethereum-roadmap/4698). These innovations are crucial in reducing transaction costs on Ethereum, but they also present new challenges, such as fragmented liquidity. In this rapidly changing landscape, leading L2 platforms are gaining prominence, and I anticipate that in the near future, a select few will handle the majority of significant transactions.

In this regard, zkSync stands out as a potential leader. Its continuous optimization positions it alongside other major L2 solutions like Optimism and Arbitrum. [Recognizing zkSync's potential to become a 'Super Rollup'](https://www.growthepie.xyz/), I developed zkSync-ETL. This tool is designed for efficient and real-time access to on-chain data, a crucial need for developers and analysts in the Ethereum ecosystem.

zkSync-ETL is an ongoing project, and warmly welcome ideas, feedback, and contributions. We ensure it remains a valuable resource for anyone looking to leverage the power of zkSync in their Ethereum-based applications.

## Architecture

### High-Level

The zkSync-ETL is structured into two primary components: the `/data` module for data storage, and the `/era` module for specific data processing tasks.

**Data Acquisition** (`/rpc` Module): This module interfaces with the [zkSync RPC](https://chainlist.org/chain/324), where running a local node is advisable (see [external node documentation](https://github.com/matter-labs/zksync-era/tree/main/docs/guides/external-node) for guidance). It retrieves raw block and transaction data in JSON format.

**Data Processing** (`/json` Module): Within the json module, raw data undergoes cleaning and processing. This transforms it into comprehensively clean data, currently comprising seven core tables: **accounts**, **balances**, **blocks**, **contracts**, **SyncSwap swaps**, **token transfers**, and **transactions**. Future updates aim to include data from mainstream DEXs, NFTs, and derivative protocols.

- accounts
- balances
- blocks
- contracts
- SyncSwap swaps
- token transfers
- transactions

**Database Management** (`/db` Module): The db module is responsible for creating PostgreSQL tables and data schemas. It imports all data in CSV format into these tables. This setup enables the development of custom data programs akin to Dune, Nansen, and The Graph, utilizing zkSync data. Additionally, these datasets can be instrumental in researching the Ethereum and zkSync ecosystems.

### Low-Level

- `/data`

  - `/json_raw_data`: Raw JSON data of blocks & transactions.
  - `/json_clean_data`: Clean JSON data of all tables.
  - `/json_to_csv`: Clean CSV data of all tables, prepare for import to PostgreSQL DB.

- `/era`

  - `/rpc`: Get raw JSON data from zkSync RPC.

    - `/fetch`: Call to get raw blocks and transactions data.
    - `/trace`: Call to get raw trace data.

  - `/json`: Convert raw JSON data to clean JSON/CSV data, and plus applications cleaner.

    - `/structures`: Define the data structure of the base tables.
    - `/resolver`: A tool that assists in converting the base table from raw data to clean data..
    - `/cleaner`: Important module to convert all raw JSON data to clean JSON and CSV data. Parsing for more applications will also be encapsulated in this module.

  - `/db`: Module for importing data into a database.

    - `/schemas`: Define the data structure of all tables in the PostgreSQL database.
    - `/exporter`: Import clean CSV data from all tables into the database.

  - `/setup`: Some basic setup.

    - `/config`: Block ranges, file size, folder size, RPC URL, etc.
    - `/tokens`: Token addresses for balance data.

  - `/utils`: All the utils crates used as dependencies of the module crates above.

## How to use it

## Contribution

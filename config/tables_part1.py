"""First part of table configurations - split due to length."""

# Pre-6780 destructs table configuration
PRE_6780_DESTRUCTS_CONFIG = {
    'pre_6780_accounts_destructs': {
        'name': 'mainnet.int_pre_6780_accounts_destructs',
        'description': 'Tracks self-destruct operations before EIP-6780',
        'source_tables': [
            'canonical_execution_traces',
            'canonical_execution_transaction',
        ],
        'create_sqls': [
            """CREATE TABLE mainnet.int_pre_6780_accounts_destructs_local on cluster '{cluster}' (
                `address` String COMMENT 'The address of the account' CODEC(ZSTD(1)),
                `block_number` UInt32 COMMENT 'The block number of the self-destructs' CODEC(ZSTD(1)),
                `transaction_hash` FixedString(66) COMMENT 'The transaction hash' CODEC(ZSTD(1)),
                `transaction_index` UInt64 COMMENT 'The transaction index' CODEC(DoubleDelta, ZSTD(1))
            ) ENGINE = ReplicatedMergeTree(
                '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
                '{replica}'
            ) PARTITION BY cityHash64(`address`) % 16
            ORDER BY (address, block_number, transaction_hash) COMMENT 'Table for accounts self-destructs data pre-6780 (Dencun fork)'""",
            
            """CREATE TABLE mainnet.int_pre_6780_accounts_destructs ON CLUSTER '{cluster}' AS mainnet.int_pre_6780_accounts_destructs_local 
            ENGINE = Distributed('{cluster}', 'mainnet', int_pre_6780_accounts_destructs_local, cityHash64(`address`))"""
        ]
    },
}

# Post-6780 destructs table configuration
POST_6780_DESTRUCTS_CONFIG = {
    'post_6780_accounts_destructs': {
        'name': 'mainnet.int_post_6780_accounts_destructs',
        'description': 'Tracks self-destruct operations after EIP-6780',
        'source_tables': [
            'canonical_execution_traces',
            'canonical_execution_contracts',
            'canonical_execution_transaction',
        ],
        'create_sqls': [
            """CREATE TABLE mainnet.int_post_6780_accounts_destructs_local on cluster '{cluster}' (
                `address` String COMMENT 'The address of the account' CODEC(ZSTD(1)),
                `block_number` UInt32 COMMENT 'The block number' CODEC(ZSTD(1)),
                `transaction_hash` FixedString(66) COMMENT 'The transaction hash' CODEC(ZSTD(1)),
                `transaction_index` UInt64 COMMENT 'The transaction index' CODEC(DoubleDelta, ZSTD(1)),
                `is_same_tx` Bool COMMENT 'Whether the self-destruct is in the same transaction as the creation' CODEC(ZSTD(1))
            ) ENGINE = ReplicatedMergeTree(
                '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
                '{replica}'
            ) PARTITION BY cityHash64(`address`) % 16
            ORDER BY (address, block_number, transaction_hash) COMMENT 'Table for accounts self-destructs data post-6780 (Dencun fork)'""",
            
            """CREATE TABLE mainnet.int_post_6780_accounts_destructs ON CLUSTER '{cluster}' AS mainnet.int_post_6780_accounts_destructs_local 
            ENGINE = Distributed('{cluster}', 'mainnet', int_post_6780_accounts_destructs_local, cityHash64(`address`))"""
        ]
    },
}

# Accounts alive table configuration
ACCOUNTS_ALIVE_CONFIG = {
    'accounts_alive': {
        'name': 'mainnet.int_accounts_alive',
        'description': 'Determines account alive status based on diffs and destructs',
        'source_tables': [
            'mainnet.int_address_diffs',
            'mainnet.int_post_6780_accounts_destructs',
            'mainnet.int_pre_6780_accounts_destructs',
        ],
        'create_sqls': [
            """CREATE TABLE mainnet.int_accounts_alive_local on cluster '{cluster}' (
                `address` String COMMENT 'The address of the account' CODEC(ZSTD(1)),
                `block_number` UInt32 COMMENT 'The block number of the latest status of this address' CODEC(ZSTD(1)),
                `is_alive` Bool COMMENT 'Whether the account is currently alive in the state' CODEC(ZSTD(1))
            ) ENGINE = ReplicatedReplacingMergeTree(
                '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
                '{replica}',
                `block_number`
            ) PARTITION BY cityHash64(`address`) % 16
            ORDER BY (address) COMMENT 'Table that states if an account is currently alive or not'""",
            
            """CREATE TABLE mainnet.int_accounts_alive ON CLUSTER '{cluster}' AS mainnet.int_accounts_alive_local 
            ENGINE = Distributed('{cluster}', 'mainnet', int_accounts_alive_local, cityHash64(`address`))"""
        ]
    },
}

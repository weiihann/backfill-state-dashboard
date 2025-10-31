CREATE TABLE default.reth_plain_accounts_local ON CLUSTER '{cluster}' (
    address String,
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}'
) ORDER BY address;

CREATE TABLE default.reth_plain_accounts ON CLUSTER '{cluster}' AS default.reth_plain_accounts_local ENGINE = Distributed('{cluster}', default, reth_plain_accounts_local, cityHash64(address));

CREATE TABLE default.reth_bytecodes_local ON CLUSTER '{cluster}' (
    codeHash String,
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}'
) ORDER BY codeHash;

CREATE TABLE default.reth_bytecodes ON CLUSTER '{cluster}' AS default.reth_bytecodes_local ENGINE = Distributed('{cluster}', default, reth_bytecodes_local, cityHash64(codeHash));
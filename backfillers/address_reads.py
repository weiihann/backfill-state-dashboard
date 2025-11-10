"""Backfiller for address_reads table."""

from typing import List
from core.backfiller import BaseBackfiller


class AddressReadsBackfiller(BaseBackfiller):
    """Backfiller for int_address_reads table."""
    
    @property
    def table_name(self) -> str:
        return "mainnet.int_address_reads"
    
    @property
    def source_tables(self) -> List[str]:
        return [
            "canonical_execution_balance_reads",
            "canonical_execution_nonce_reads",
            "canonical_execution_storage_reads",
            "canonical_execution_transaction",
        ]
    
    @property
    def description(self) -> str:
        return "Xatu Int Address Reads Backfill"
    
    def generate_sql(self, start_block: int, end_block: int) -> str:
        """Generate the SQL query for backfilling a block range."""
        return f"""
INSERT INTO {self.table_name}
WITH
get_tx_success AS (
    SELECT 
        lower(transaction_hash) AS transaction_hash,
        transaction_index
    FROM default.canonical_execution_transaction FINAL
    WHERE block_number BETWEEN {start_block} AND {end_block}
    AND success = true
),
all_address_reads AS (
    SELECT
        lower(address) AS address,
        block_number,
        lower(transaction_hash) AS transaction_hash
    FROM default.canonical_execution_balance_reads FINAL
    WHERE block_number BETWEEN {start_block} AND {end_block}

    UNION ALL

    SELECT
        lower(address) AS address,
        block_number,
        lower(transaction_hash) AS transaction_hash
    FROM default.canonical_execution_nonce_reads FINAL
    WHERE block_number BETWEEN {start_block} AND {end_block}

    UNION ALL

    SELECT
        lower(contract_address) AS address,
        block_number,
        lower(transaction_hash) AS transaction_hash
    FROM default.canonical_execution_storage_reads FINAL
    WHERE block_number BETWEEN {start_block} AND {end_block}
),
address_reads AS (
    SELECT
        ar.address,
        ar.block_number,
        ar.transaction_hash,
        g.transaction_index
    FROM all_address_reads ar
    GLOBAL JOIN get_tx_success g
        ON ar.transaction_hash = g.transaction_hash
)
SELECT
    address,
    block_number,
    countDistinct(transaction_hash) AS tx_count,
    max(transaction_index) AS last_tx_index
FROM address_reads
GROUP BY address, block_number
"""

"""Backfiller for address_last_access table."""

from typing import List
from core.backfiller import BaseBackfiller


class AddressLastAccessBackfiller(BaseBackfiller):
    """Backfiller for int_address_last_access table."""
    
    @property
    def table_name(self) -> str:
        return "mainnet.int_address_last_access"
    
    @property
    def source_tables(self) -> List[str]:
        return [
            "canonical_execution_balance_diffs",
            "canonical_execution_balance_reads",
            "canonical_execution_contracts",
            "canonical_execution_nonce_reads",
            "canonical_execution_nonce_diffs",
            "canonical_execution_storage_diffs",
            "canonical_execution_storage_reads",
        ]
    
    @property
    def description(self) -> str:
        return "Xatu Int Address Last Access Backfill"
    
    def generate_sql(self, start_block: int, end_block: int) -> str:
        """Generate the SQL query for backfilling a block range."""
        return f"""
INSERT INTO {self.table_name}
WITH
get_tx_success AS (
    SELECT lower(transaction_hash) AS transaction_hash
    FROM default.canonical_execution_transaction FINAL
    WHERE block_number BETWEEN {start_block} AND {end_block}
    AND success = true
),
all_addresses AS (
    SELECT lower(address) AS address, lower(transaction_hash) AS transaction_hash, block_number
    FROM default.canonical_execution_nonce_reads FINAL
    WHERE block_number BETWEEN {start_block} AND {end_block}

    UNION ALL

    SELECT lower(address) AS address, lower(transaction_hash) AS transaction_hash, block_number
    FROM default.canonical_execution_nonce_diffs FINAL
    WHERE block_number BETWEEN {start_block} AND {end_block}

    UNION ALL

    SELECT lower(address) AS address, lower(transaction_hash) AS transaction_hash, block_number
    FROM default.canonical_execution_balance_diffs FINAL
    WHERE block_number BETWEEN {start_block} AND {end_block}

    UNION ALL

    SELECT lower(address) AS address, lower(transaction_hash) AS transaction_hash, block_number
    FROM default.canonical_execution_balance_reads FINAL
    WHERE block_number BETWEEN {start_block} AND {end_block}

    UNION ALL

    SELECT lower(address) AS address, lower(transaction_hash) AS transaction_hash, block_number
    FROM default.canonical_execution_storage_diffs FINAL
    WHERE block_number BETWEEN {start_block} AND {end_block}

    UNION ALL

    SELECT lower(contract_address) AS address, lower(transaction_hash) AS transaction_hash, block_number
    FROM default.canonical_execution_storage_reads FINAL
    WHERE block_number BETWEEN {start_block} AND {end_block}

    UNION ALL

    SELECT lower(contract_address) AS address, lower(transaction_hash) AS transaction_hash, block_number
    FROM default.canonical_execution_contracts FINAL
    WHERE block_number BETWEEN {start_block} AND {end_block}
)
SELECT
    a.address,
    max(a.block_number) AS block_number,
    false AS is_deleted  -- Default to false, actual deletion status would need to be determined from destructs
FROM all_addresses a
GLOBAL JOIN get_tx_success g
    ON a.transaction_hash = g.transaction_hash
GROUP BY a.address
"""

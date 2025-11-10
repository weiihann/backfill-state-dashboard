"""Backfiller for address_storage_slot_first_access table."""

from typing import List
from core.backfiller import BaseBackfiller


class AddressStorageSlotFirstAccessBackfiller(BaseBackfiller):
    """Backfiller for int_address_storage_slot_first_access table."""
    
    @property
    def table_name(self) -> str:
        return "mainnet.int_address_storage_slot_first_access"
    
    @property
    def source_tables(self) -> List[str]:
        return [
            "canonical_execution_storage_diffs",
            "canonical_execution_storage_reads",
            "canonical_execution_transaction",
        ]
    
    @property
    def description(self) -> str:
        return "Xatu Int Address Storage Slot First Access Backfill"
    
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
all_storage_access AS (
    SELECT 
        lower(address) AS address,
        slot AS slot_key,
        block_number,
        to_value AS value,
        lower(transaction_hash) AS transaction_hash
    FROM default.canonical_execution_storage_diffs FINAL
    WHERE block_number BETWEEN {start_block} AND {end_block}

    UNION ALL

    SELECT 
        lower(contract_address) AS address,
        slot AS slot_key,
        block_number,
        value,
        lower(transaction_hash) AS transaction_hash
    FROM default.canonical_execution_storage_reads FINAL
    WHERE block_number BETWEEN {start_block} AND {end_block}
)
SELECT
    s.address,
    s.slot_key,
    min(s.block_number) AS block_number,
    argMin(s.value, s.block_number) AS value,
    NULL AS version
FROM all_storage_access s
GLOBAL JOIN get_tx_success g
    ON s.transaction_hash = g.transaction_hash
GROUP BY s.address, s.slot_key
"""

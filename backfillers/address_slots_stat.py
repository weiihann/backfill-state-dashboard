"""Backfiller for address_slots_stat_per_block table."""

from typing import List
from core.backfiller import BaseBackfiller


class AddressSlotsStatBackfiller(BaseBackfiller):
    """Backfiller for int_address_slots_stat_per_block table."""
    
    @property
    def table_name(self) -> str:
        return "mainnet.int_address_slots_stat_per_block"
    
    @property
    def source_tables(self) -> List[str]:
        return [
            "canonical_execution_storage_diffs",
            "canonical_execution_transaction",
        ]
    
    @property
    def description(self) -> str:
        return "Xatu Int Address Slots Stat Per Block Backfill"
    
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
storage_changes AS (
    SELECT
        lower(sd.address) AS address,
        sd.block_number,
        sd.from_value,
        sd.to_value
    FROM default.canonical_execution_storage_diffs sd FINAL
    GLOBAL JOIN get_tx_success g
        ON lower(sd.transaction_hash) = g.transaction_hash
    WHERE sd.block_number BETWEEN {start_block} AND {end_block}
),
address_slot_stats AS (
    SELECT
        address,
        block_number,
        countIf(from_value != '0x0000000000000000000000000000000000000000000000000000000000000000' 
                AND to_value = '0x0000000000000000000000000000000000000000000000000000000000000000') AS slots_cleared,
        countIf(from_value = '0x0000000000000000000000000000000000000000000000000000000000000000' 
                AND to_value != '0x0000000000000000000000000000000000000000000000000000000000000000') AS slots_set
    FROM storage_changes
    GROUP BY address, block_number
)
SELECT
    address,
    block_number,
    slots_cleared,
    slots_set,
    NULL AS net_slots,
    NULL AS net_slots_bytes
FROM address_slot_stats
"""

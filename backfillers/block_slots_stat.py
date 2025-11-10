"""Backfiller for block_slots_stat table."""

from typing import List
from core.backfiller import BaseBackfiller


class BlockSlotsStatBackfiller(BaseBackfiller):
    """Backfiller for int_block_slots_stat table."""
    
    @property
    def table_name(self) -> str:
        return "mainnet.int_block_slots_stat"
    
    @property
    def source_tables(self) -> List[str]:
        return [
            "canonical_execution_storage_diffs",
            "canonical_execution_transaction",
        ]
    
    @property
    def description(self) -> str:
        return "Xatu Int Block Slots Stat Backfill"
    
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
        sd.block_number,
        sd.from_value,
        sd.to_value
    FROM default.canonical_execution_storage_diffs sd FINAL
    GLOBAL JOIN get_tx_success g
        ON lower(sd.transaction_hash) = g.transaction_hash
    WHERE sd.block_number BETWEEN {start_block} AND {end_block}
),
block_slot_stats AS (
    SELECT
        block_number,
        countIf(from_value != '0x0000000000000000000000000000000000000000000000000000000000000000' 
                AND to_value = '0x0000000000000000000000000000000000000000000000000000000000000000') AS slots_cleared,
        countIf(from_value = '0x0000000000000000000000000000000000000000000000000000000000000000' 
                AND to_value != '0x0000000000000000000000000000000000000000000000000000000000000000') AS slots_set
    FROM storage_changes
    GROUP BY block_number
)
SELECT
    block_number,
    slots_cleared,
    slots_set,
    NULL AS net_slots,
    NULL AS net_slots_bytes
FROM block_slot_stats
"""

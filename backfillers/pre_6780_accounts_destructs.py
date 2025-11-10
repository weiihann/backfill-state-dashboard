"""Backfiller for pre-EIP-6780 accounts destructs table."""

from typing import List, Dict, Any
from core.backfiller import BaseBackfiller

# EIP-6780 activation block on mainnet
EIP_6780_BLOCK = 19426587
# EIP-161 activation block on mainnet (Spurious Dragon)
EIP_161_BLOCK = 2675000


class Pre6780AccountsDestructsBackfiller(BaseBackfiller):
    """Backfiller for int_pre_6780_accounts_destructs table."""
    
    @property
    def table_name(self) -> str:
        return "mainnet.int_pre_6780_accounts_destructs"
    
    @property
    def source_tables(self) -> List[str]:
        return [
            "canonical_execution_traces",
            "canonical_execution_transaction",
        ]
    
    @property
    def description(self) -> str:
        return "Xatu Pre-EIP6780 Accounts Destructs Backfill"
    
    def get_additional_info(self) -> Dict[str, Any]:
        return {
            "EIP-6780 block": EIP_6780_BLOCK,
            "EIP-161 block": EIP_161_BLOCK,
        }
    
    def get_block_range_note(self, start_block: int, end_block: int) -> str:
        """Add note about EIP-161 blocks."""
        if end_block < EIP_161_BLOCK:
            return " (pre-EIP-161)"
        return ""
    
    def generate_sql(self, start_block: int, end_block: int) -> str:
        """Generate the SQL query for backfilling a block range."""
        # Only process blocks before EIP-6780
        if start_block >= EIP_6780_BLOCK:
            return ""
        
        # Cap end_block at EIP-6780 block
        if end_block >= EIP_6780_BLOCK:
            end_block = EIP_6780_BLOCK - 1
        
        return f"""
INSERT INTO {self.table_name}
WITH
get_tx_success AS (
    SELECT
        lower(transaction_hash) AS transaction_hash,
        transaction_index
    FROM default.canonical_execution_transaction FINAL
    WHERE
        block_number < {EIP_6780_BLOCK} 
        AND block_number BETWEEN {start_block} AND {end_block}
        AND success = true
),
pre_eip161_empty_accounts AS (
    SELECT 
        lower(action_to) AS address,
        block_number,
        lower(transaction_hash) AS tx_hash
    FROM canonical_execution_traces FINAL
    WHERE
        action_type='suicide'
        AND block_number < {EIP_161_BLOCK}
        AND block_number BETWEEN {start_block} AND {end_block}
        AND action_value='0'
),
self_destructs AS (
    SELECT
        lower(action_from) AS address,
        block_number,
        lower(transaction_hash) AS tx_hash
    FROM canonical_execution_traces FINAL
    WHERE
        action_type='suicide'
        AND block_number < {EIP_6780_BLOCK}
        AND block_number BETWEEN {start_block} AND {end_block}
    
    UNION ALL
    
    SELECT address, block_number, tx_hash FROM pre_eip161_empty_accounts
)
SELECT
    s.address,
    s.block_number,
    s.tx_hash,
    max(g.transaction_index)
FROM self_destructs s
GLOBAL JOIN get_tx_success g
ON s.tx_hash = g.transaction_hash
GROUP BY s.address, s.block_number, s.tx_hash
"""

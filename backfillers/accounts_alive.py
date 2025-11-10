"""Backfiller for accounts_alive table."""

from typing import List, Dict, Any
from core.backfiller import BaseBackfiller

# EIP-6780 activation block on mainnet
EIP_6780_BLOCK = 19426587


class AccountsAliveBackfiller(BaseBackfiller):
    """Backfiller for int_accounts_alive table."""
    
    @property
    def table_name(self) -> str:
        return "mainnet.int_accounts_alive"
    
    @property
    def source_tables(self) -> List[str]:
        return [
            "mainnet.int_address_diffs",
            "mainnet.int_post_6780_accounts_destructs",
            "mainnet.int_pre_6780_accounts_destructs",
        ]
    
    @property
    def description(self) -> str:
        return "Xatu Accounts Alive Status Backfill"
    
    def get_additional_info(self) -> Dict[str, Any]:
        return {
            "EIP-6780 block": EIP_6780_BLOCK,
            "Logic": "Using argMax to get latest status by (block_number, transaction_index)"
        }
    
    def get_block_range_note(self, start_block: int, end_block: int) -> str:
        """Add note about EIP-6780 blocks."""
        if end_block < EIP_6780_BLOCK:
            return " (pre-EIP-6780)"
        elif start_block >= EIP_6780_BLOCK:
            return " (post-EIP-6780)"
        elif start_block < EIP_6780_BLOCK and end_block >= EIP_6780_BLOCK:
            return " (spans EIP-6780)"
        return ""
    
    def generate_sql(self, start_block: int, end_block: int) -> str:
        """Generate the SQL query for backfilling a block range."""
        return f"""
INSERT INTO {self.table_name}
WITH
-- Pre-6780 destructs: always mark as dead
pre_6780_destructs AS (
    SELECT
        address,
        block_number as block_num,
        transaction_index,
        false AS is_alive
    FROM mainnet.int_pre_6780_accounts_destructs
    WHERE block_number BETWEEN {start_block} AND {end_block}
),
-- Post-6780 destructs: dead only if is_same_tx = true
post_6780_destructs AS (
    SELECT
        address,
        block_number as block_num,
        transaction_index,
        CASE 
            WHEN is_same_tx = true THEN false  -- Account actually destroyed
            ELSE true                           -- Account not destroyed, just cleared
        END AS is_alive
    FROM mainnet.int_post_6780_accounts_destructs
    WHERE block_number BETWEEN {start_block} AND {end_block}
),
-- Diffs: always mark as alive (use existing last_tx_index column)
diffs AS (
    SELECT
        address,
        block_number as block_num,
        last_tx_index AS transaction_index,
        true AS is_alive
    FROM mainnet.int_address_diffs
    WHERE block_number BETWEEN {start_block} AND {end_block}
),
-- Combine all events
combined AS (
    SELECT * FROM pre_6780_destructs
    UNION ALL
    SELECT * FROM post_6780_destructs
    UNION ALL
    SELECT * FROM diffs
)
-- Get the latest status for each address
-- When there are ties (same block_number, transaction_index), pessimistically choose false
SELECT
    address,
    max(block_num) AS block_number,
    argMax(is_alive, (block_num, transaction_index, NOT is_alive)) AS is_alive
FROM combined
GROUP BY address
"""

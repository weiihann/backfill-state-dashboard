"""Backfiller for post-EIP-6780 accounts destructs table."""

from typing import List, Dict, Any
from core.backfiller import BaseBackfiller

# EIP-6780 activation block on mainnet
EIP_6780_BLOCK = 19426587


class Post6780AccountsDestructsBackfiller(BaseBackfiller):
    """Backfiller for int_post_6780_accounts_destructs table."""
    
    @property
    def table_name(self) -> str:
        return "mainnet.int_post_6780_accounts_destructs"
    
    @property
    def source_tables(self) -> List[str]:
        return [
            "canonical_execution_traces",
            "canonical_execution_contracts",
            "canonical_execution_transaction",
        ]
    
    @property
    def description(self) -> str:
        return "Xatu Post-EIP6780 Accounts Destructs Backfill"
    
    def get_additional_info(self) -> Dict[str, Any]:
        return {
            "EIP-6780 block": EIP_6780_BLOCK,
        }
    
    def generate_sql(self, start_block: int, end_block: int) -> str:
        """Generate the SQL query for backfilling a block range."""
        # Only process blocks after EIP-6780
        if end_block < EIP_6780_BLOCK:
            return ""
        
        # Adjust start_block if needed
        if start_block < EIP_6780_BLOCK:
            start_block = EIP_6780_BLOCK
        
        return f"""
INSERT INTO {self.table_name}
WITH
get_tx_success AS (
    SELECT
        lower(transaction_hash) AS transaction_hash,
        transaction_index
    FROM default.canonical_execution_transaction FINAL
    WHERE
        block_number >= {EIP_6780_BLOCK}
        AND block_number BETWEEN {start_block} AND {end_block}
        AND success = true
),
contracts AS (
    SELECT
        lower(contract_address) AS address,
        lower(transaction_hash) AS tx_hash
    FROM default.canonical_execution_contracts FINAL
    WHERE block_number BETWEEN {start_block} AND {end_block}
),
self_destructs AS (
    SELECT
        lower(t.action_from) AS address,
        t.block_number,
        lower(t.transaction_hash) AS tx_hash,
        CASE 
            WHEN c.address IS NOT NULL THEN true
            ELSE false
        END AS is_same_tx
    FROM canonical_execution_traces t FINAL
    LEFT JOIN contracts c ON t.action_from = c.address AND t.transaction_hash = c.tx_hash
    WHERE
        t.action_type = 'suicide'
        AND t.block_number >= {EIP_6780_BLOCK}
        AND t.block_number BETWEEN {start_block} AND {end_block}
)
SELECT
    s.address,
    s.block_number,
    s.tx_hash AS transaction_hash,
    max(g.transaction_index) AS transaction_index,
    any(s.is_same_tx) AS is_same_tx
FROM self_destructs s
GLOBAL JOIN get_tx_success g
ON s.tx_hash = g.transaction_hash
GROUP BY s.address, s.block_number, s.tx_hash
"""

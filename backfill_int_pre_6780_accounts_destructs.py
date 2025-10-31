"""
Backfill script for int_pre_6780_accounts_destructs table.

This script fills the mainnet.int_pre_6780_accounts_destructs table by tracking
self-destruct operations from execution traces that occurred before EIP-6780
(block 19426587), including special handling for pre-EIP161 empty accounts.
"""

import os
import time
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from utils import get_block_range

load_dotenv()

username = os.getenv('XATU_CLICKHOUSE_USERNAME')
password = os.getenv('XATU_CLICKHOUSE_PASSWORD')
url = os.getenv('XATU_CLICKHOUSE_URL')
protocol = os.getenv('XATU_CLICKHOUSE_PROTOCOL')

db_url = f"clickhouse+http://{username}:{password}@{url}/default?protocol={protocol}"
engine = create_engine(db_url)

# Configuration
TARGET_TABLE = "mainnet.int_pre_6780_accounts_destructs"
SOURCE_TABLES = [
    "canonical_execution_traces",
    "canonical_execution_transaction",
]
# EIP-6780 activation block on mainnet
EIP_6780_BLOCK = 19426587
# EIP-161 activation block on mainnet (Spurious Dragon)
EIP_161_BLOCK = 2675000


def generate_backfill_sql(start_block: int, end_block: int) -> str:
    """Generate the SQL query for backfilling a block range."""
    # Only process blocks before EIP-6780
    if start_block >= EIP_6780_BLOCK:
        return ""
    
    # Cap end_block at EIP-6780 block
    if end_block >= EIP_6780_BLOCK:
        end_block = EIP_6780_BLOCK - 1
    
    return f"""
INSERT INTO {TARGET_TABLE}
WITH
get_tx_success AS (
    SELECT
        lower(transaction_hash) AS transaction_hash,
        transaction_index
    FROM default.canonical_execution_transaction FINAL
    WHERE
        block_number < {EIP_6780_BLOCK} 
        block_number BETWEEN {start_block} AND {end_block}
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


def execute_backfill(start_block: int, end_block: int, step: int = 10000) -> None:
    """
    Execute the backfill operation in chunks.

    Args:
        start_block: Starting block number
        end_block: Ending block number
        step: Number of blocks to process in each chunk
    """
    # Check if we need to process any blocks
    if start_block >= EIP_6780_BLOCK:
        print(f"\nNo blocks to process (start_block {start_block} >= EIP-6780 block {EIP_6780_BLOCK})")
        return
    
    # Cap end_block at EIP-6780
    if end_block >= EIP_6780_BLOCK:
        print(f"\nCapping end_block at {EIP_6780_BLOCK - 1} (EIP-6780 activation)")
        end_block = EIP_6780_BLOCK - 1
    
    if end_block <= start_block:
        print(f"\nNo blocks to process (end_block: {end_block} <= start_block: {start_block})")
        return

    total_blocks = end_block - start_block + 1
    blocks_processed = 0

    print(f"\nProcessing {total_blocks} blocks in steps of {step}")
    print(f"Block range: {start_block} to {end_block}")
    print("=" * 80)

    backfill_start_time = time.time()

    with engine.begin() as conn:
        for lower in range(start_block, end_block + 1, step):
            upper = min(lower + step - 1, end_block)

            chunk_start_time = time.time()
            sql = generate_backfill_sql(lower, upper)
            
            if sql:  # Only execute if we have a valid query
                conn.execute(text(sql))
                chunk_end_time = time.time()

                blocks_processed += (upper - lower + 1)
                progress = (blocks_processed / total_blocks) * 100
                chunk_time = chunk_end_time - chunk_start_time

                # Note pre-EIP161 blocks for reference
                eip161_note = " (pre-EIP161)" if upper < EIP_161_BLOCK else ""
                
                print(f"Processed blocks {lower:>10} - {upper:>10}{eip161_note} | "
                      f"Progress: {progress:>6.2f}% | "
                      f"Time: {chunk_time:>6.2f}s")

    backfill_end_time = time.time()
    total_time = backfill_end_time - backfill_start_time

    print("=" * 80)
    print(f"\nBackfill completed!")
    print(f"  Total blocks processed: {blocks_processed}")
    print(f"  Total time: {total_time:.2f}s")
    print(f"  Average speed: {blocks_processed/total_time:.2f} blocks/sec")


def main():
    """Main backfill function."""
    print("=" * 80)
    print("Xatu Pre-EIP6780 Accounts Destructs Backfill")
    print("=" * 80)

    # Get block range
    start_block, end_block = get_block_range(TARGET_TABLE, SOURCE_TABLES, engine)

    # Configuration
    step_size = 10000  # Process 10k blocks at a time

    print(f"\nConfiguration:")
    print(f"  Target table: {TARGET_TABLE}")
    print(f"  Step size: {step_size}")
    print(f"  EIP-6780 block: {EIP_6780_BLOCK}")
    print(f"  EIP-161 block: {EIP_161_BLOCK}")

    # Execute backfill
    execute_backfill(start_block, end_block, step_size)


if __name__ == "__main__":
    main()

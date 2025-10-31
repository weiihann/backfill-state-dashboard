"""
Backfill script for int_post_6780_accounts_destructs table.

This script fills the mainnet.int_post_6780_accounts_destructs table by tracking
self-destruct operations from execution traces that occurred after EIP-6780
(block 19426587), identifying whether each destruct happened in the same
transaction as the address creation.
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
TARGET_TABLE = "mainnet.int_post_6780_accounts_destructs"
SOURCE_TABLES = [
    "canonical_execution_address_appearances",
    "canonical_execution_transaction",
]
# EIP-6780 activation block on mainnet (Dencun fork)
EIP_6780_BLOCK = 19426587


def generate_backfill_sql(start_block: int, end_block: int) -> str:
    """Generate the SQL query for backfilling a block range."""
    # Only process blocks from EIP-6780 onwards
    if end_block < EIP_6780_BLOCK:
        return ""
    
    # Start from EIP-6780 block if needed
    if start_block < EIP_6780_BLOCK:
        start_block = EIP_6780_BLOCK
    
    return f"""
INSERT INTO {TARGET_TABLE}
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
address_events AS (
    SELECT
        lower(address) AS address,
        block_number,
        lower(transaction_hash) AS transaction_hash,
        max(CASE WHEN relationship = 'create' THEN 1 ELSE 0 END) AS has_create,
        max(CASE WHEN relationship = 'suicide' THEN 1 ELSE 0 END) AS has_suicide
    FROM default.canonical_execution_address_appearances FINAL
    WHERE
        block_number >= {EIP_6780_BLOCK}
        AND block_number BETWEEN {start_block} AND {end_block}
        AND relationship IN ('create', 'suicide')
    GROUP BY 
        address,
        block_number,
        transaction_hash
)
SELECT
    ae.address,
    ae.block_number,
    ae.transaction_hash,
    g.transaction_index,
    CASE 
        WHEN ae.has_create = 1 AND ae.has_suicide = 1 THEN true
        ELSE false
    END AS is_same_tx
FROM address_events ae
GLOBAL JOIN get_tx_success g
    ON ae.transaction_hash = g.transaction_hash
WHERE ae.has_suicide = 1
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
    if end_block < EIP_6780_BLOCK:
        print(f"\nNo blocks to process (end_block {end_block} < EIP-6780 block {EIP_6780_BLOCK})")
        return
    
    # Start from EIP-6780 if needed
    if start_block < EIP_6780_BLOCK:
        print(f"\nAdjusting start_block from {start_block} to {EIP_6780_BLOCK} (EIP-6780 activation)")
        start_block = EIP_6780_BLOCK
    
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
                
                print(f"Processed blocks {lower:>10} - {upper:>10} | "
                      f"Progress: {progress:>6.2f}% | "
                      f"Time: {chunk_time:>6.2f}s")

    backfill_end_time = time.time()
    total_time = backfill_end_time - backfill_start_time

    print("=" * 80)
    print(f"\nBackfill completed!")
    print(f"  Total blocks processed: {blocks_processed}")
    print(f"  Total time: {total_time:.2f}s")
    if total_time > 0:
        print(f"  Average speed: {blocks_processed/total_time:.2f} blocks/sec")


def main():
    """Main backfill function."""
    print("=" * 80)
    print("Xatu Post-EIP6780 Accounts Destructs Backfill")
    print("=" * 80)

    # Get block range
    start_block, end_block = get_block_range(TARGET_TABLE, SOURCE_TABLES, engine)

    # Configuration
    step_size = 10000  # Process 10k blocks at a time

    print(f"\nConfiguration:")
    print(f"  Target table: {TARGET_TABLE}")
    print(f"  Step size: {step_size}")
    print(f"  EIP-6780 block: {EIP_6780_BLOCK}")
    print(f"  Processing from: Block {EIP_6780_BLOCK} onwards")

    # Execute backfill
    execute_backfill(start_block, end_block, step_size)


if __name__ == "__main__":
    main()

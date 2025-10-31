"""
Backfill script for int_accounts_alive table.

This script fills the mainnet.int_accounts_alive table by combining all account
events (diffs and destructs) and using argMax to determine the final alive status
based on the most recent event (by block_number and transaction_index).
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
TARGET_TABLE = "mainnet.int_accounts_alive"
# Only use tables that span the full range for determining end block
# Pre-6780 destructs only exists up to block 19426586
SOURCE_TABLES = [
    "mainnet.int_address_diffs",
    "mainnet.int_post_6780_accounts_destructs",
]
# EIP-6780 activation block on mainnet
EIP_6780_BLOCK = 19426587


def generate_backfill_sql(start_block: int, end_block: int) -> str:
    """Generate the SQL query for backfilling a block range."""
    return f"""
INSERT INTO {TARGET_TABLE}
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


def execute_backfill(start_block: int, end_block: int, step: int = 10000) -> None:
    """
    Execute the backfill operation in chunks.

    Args:
        start_block: Starting block number
        end_block: Ending block number
        step: Number of blocks to process in each chunk
    """
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
            conn.execute(text(sql))
            chunk_end_time = time.time()

            blocks_processed += (upper - lower + 1)
            progress = (blocks_processed / total_blocks) * 100
            chunk_time = chunk_end_time - chunk_start_time

            # Note if processing pre/post EIP-6780 blocks
            eip_note = ""
            if upper < EIP_6780_BLOCK:
                eip_note = " (pre-EIP-6780)"
            elif lower >= EIP_6780_BLOCK:
                eip_note = " (post-EIP-6780)"
            elif lower < EIP_6780_BLOCK and upper >= EIP_6780_BLOCK:
                eip_note = " (spans EIP-6780)"

            print(f"Processed blocks {lower:>10} - {upper:>10}{eip_note} | "
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
    print("Xatu Accounts Alive Status Backfill")
    print("=" * 80)

    # Get block range
    start_block, end_block = get_block_range(TARGET_TABLE, SOURCE_TABLES, engine)

    # Configuration
    step_size = 10000  # Process 10k blocks at a time

    print(f"\nConfiguration:")
    print(f"  Target table: {TARGET_TABLE}")
    print(f"  Source tables for range:")
    for table in SOURCE_TABLES:
        print(f"    - {table}")
    print(f"  Additional source table:")
    print(f"    - mainnet.int_pre_6780_accounts_destructs (blocks < {EIP_6780_BLOCK})")
    print(f"  Step size: {step_size}")
    print(f"  EIP-6780 block: {EIP_6780_BLOCK}")
    print(f"\nAlive determination logic:")
    print(f"  Using argMax to get latest status by (block_number, transaction_index)")
    print(f"  - Diffs -> alive")
    print(f"  - Pre-6780 destructs -> dead")
    print(f"  - Post-6780 destructs:")
    print(f"    * same-tx destruct -> dead")
    print(f"    * regular destruct -> alive")

    # Execute backfill
    execute_backfill(start_block, end_block, step_size)


if __name__ == "__main__":
    main()

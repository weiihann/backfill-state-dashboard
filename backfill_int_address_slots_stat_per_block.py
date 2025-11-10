"""
Backfill script for int_address_slots_stat_per_block table.

This script fills the mainnet.int_address_slots_stat_per_block table by analyzing
storage_diffs data to count the number of storage slots cleared and set for each
address per block, considering only successful transactions.
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
TARGET_TABLE = "mainnet.int_address_slots_stat_per_block"
SOURCE_TABLES = [
    "canonical_execution_storage_diffs",
    "canonical_execution_transaction",
]


def generate_backfill_sql(start_block: int, end_block: int) -> str:
    """Generate the SQL query for backfilling a block range."""
    return f"""
INSERT INTO {TARGET_TABLE}
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

            print(f"Processed blocks {lower:>10} - {upper:>10} | "
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
    print("Xatu Int Address Slots Stat Per Block Backfill")
    print("=" * 80)

    # Get block range
    start_block, end_block = get_block_range(TARGET_TABLE, SOURCE_TABLES, engine)

    # Configuration
    step_size = 10000  # Process 10k blocks at a time

    print(f"\nConfiguration:")
    print(f"  Target table: {TARGET_TABLE}")
    print(f"  Step size: {step_size}")

    # Execute backfill
    execute_backfill(start_block, end_block, step_size)


if __name__ == "__main__":
    main()

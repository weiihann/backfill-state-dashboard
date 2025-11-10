"""
Backfill script for int_block_slots_stat table.

This script fills the mainnet.int_block_slots_stat table by aggregating
slot statistics from mainnet.int_address_slots_stat_per_block, summing up
the slots cleared and set across all addresses for each block.
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
TARGET_TABLE = "mainnet.int_block_slots_stat"
SOURCE_TABLES = [
    "mainnet.int_address_slots_stat_per_block",
]


def generate_backfill_sql(start_block: int, end_block: int) -> str:
    """Generate the SQL query for backfilling a block range."""
    return f"""
INSERT INTO {TARGET_TABLE}
SELECT
    block_number,
    sum(slots_cleared) AS slots_cleared,
    sum(slots_set) AS slots_set,
    NULL AS net_slots,
    NULL AS net_slots_bytes
FROM mainnet.int_address_slots_stat_per_block
WHERE block_number BETWEEN {start_block} AND {end_block}
GROUP BY block_number
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
    print("Xatu Int Block Slots Stat Backfill")
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

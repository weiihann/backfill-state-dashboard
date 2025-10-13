"""
Backfill script for int_address_first_access table.

This script fills the mainnet.int_address_first_access table by combining data from
multiple source tables and determining the first access block for each address.
"""

import os
import time
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from utils import get_max_block_from_table, get_min_block_from_source_tables, get_block_range

load_dotenv()

username = os.getenv('XATU_CLICKHOUSE_USERNAME')
password = os.getenv('XATU_CLICKHOUSE_PASSWORD')
url = os.getenv('XATU_CLICKHOUSE_URL')
protocol = os.getenv('XATU_CLICKHOUSE_PROTOCOL')

db_url = f"clickhouse+http://{username}:{password}@{url}/default?protocol={protocol}"
engine = create_engine(db_url)

# Configuration
TARGET_TABLE = "mainnet.int_address_first_access"
SOURCE_TABLES = [
    "canonical_execution_balance_diffs",
    "canonical_execution_balance_reads",
    "canonical_execution_contracts",
    "canonical_execution_nonce_reads",
    "canonical_execution_nonce_diffs",
    "canonical_execution_storage_diffs",
    "canonical_execution_storage_reads",
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
all_addresses AS (
    SELECT lower(address) AS address, lower(transaction_hash) AS transaction_hash, block_number
    FROM default.canonical_execution_nonce_reads FINAL
    WHERE block_number BETWEEN {start_block} AND {end_block}

    UNION ALL

    SELECT lower(address) AS address, lower(transaction_hash) AS transaction_hash, block_number
    FROM default.canonical_execution_nonce_diffs FINAL
    WHERE block_number BETWEEN {start_block} AND {end_block}

    UNION ALL

    SELECT lower(address) AS address, lower(transaction_hash) AS transaction_hash, block_number
    FROM default.canonical_execution_balance_diffs FINAL
    WHERE block_number BETWEEN {start_block} AND {end_block}

    UNION ALL

    SELECT lower(address) AS address, lower(transaction_hash) AS transaction_hash, block_number
    FROM default.canonical_execution_balance_reads FINAL
    WHERE block_number BETWEEN {start_block} AND {end_block}

    UNION ALL

    SELECT lower(address) AS address, lower(transaction_hash) AS transaction_hash, block_number
    FROM default.canonical_execution_storage_diffs FINAL
    WHERE block_number BETWEEN {start_block} AND {end_block}

    UNION ALL

    SELECT lower(contract_address) AS address, lower(transaction_hash) AS transaction_hash, block_number
    FROM default.canonical_execution_storage_reads FINAL
    WHERE block_number BETWEEN {start_block} AND {end_block}

    UNION ALL

    SELECT lower(contract_address) AS address, lower(transaction_hash) AS transaction_hash, block_number
    FROM default.canonical_execution_contracts FINAL
    WHERE block_number BETWEEN {start_block} AND {end_block}
)
SELECT
    a.address,
    min(a.block_number) AS block_number,
    NULL AS version
FROM all_addresses a
GLOBAL JOIN get_tx_success g
    ON a.transaction_hash = g.transaction_hash
GROUP BY a.address
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
    print("Xatu Int Address First Access Backfill")
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

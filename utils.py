from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from typing import Tuple

def get_max_block_from_table(table_name: str, engine) -> int:
    """Get the maximum block number from a given table."""
    with engine.begin() as conn:
        result = conn.execute(text(f"SELECT MAX(block_number) FROM {table_name}"))
        max_block = result.scalar()
        return max_block if max_block is not None else 0


def get_min_block_from_source_tables(engine, source_tables: list[str]) -> int:
    """Get the minimum block number across all source tables."""
    min_block = None

    for table in source_tables:
        try:
            with engine.begin() as conn:
                result = conn.execute(text(f"SELECT MAX(block_number) FROM {table}"))
                max_block = result.scalar()
                max_block = max_block if max_block is not None else 0
            print(f"Table {table}: max block = {max_block}")
            if min_block is None or max_block < min_block:
                min_block = max_block
        except Exception as e:
            print(f"Warning: Could not get max block from {table}: {e}")
            continue

    return min_block if min_block is not None else 0

def get_block_range(target_table: str, source_tables: list[str], engine) -> Tuple[int, int]:
    """
    Determine the block range for backfilling.

    Returns:
        Tuple of (start_block, end_block)
        - start_block: max block_number in target table
        - end_block: minimum max block across all source tables
    """
    start_block = get_max_block_from_table(target_table, engine)
    end_block = get_min_block_from_source_tables(engine, source_tables)

    print(f"\nBlock range determined:")
    print(f"  Start block (max in {target_table}): {start_block}")
    print(f"  End block (min max across sources): {end_block}")

    return start_block, end_block
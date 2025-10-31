"""
Backfill script for reth_plain_accounts table.

This script loads parquet files from a local reth_plain_accounts folder into the 
reth_plain_accounts table. The parquet files contain Ethereum address data 
exported from Reth node state.
"""

import os
import time
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
import clickhouse_connect
from typing import Optional, List

load_dotenv()

# Database configuration
username = os.getenv('XATU_CLICKHOUSE_USERNAME')
password = os.getenv('XATU_CLICKHOUSE_PASSWORD')
url_host = os.getenv('XATU_CLICKHOUSE_URL')
protocol = os.getenv('XATU_CLICKHOUSE_PROTOCOL')

# Create ClickHouse client
client = clickhouse_connect.get_client(
    host=url_host,
    username=username,
    password=password,
    secure=(protocol == 'https')
)

# Configuration
TARGET_TABLE = "default.reth_plain_accounts"
PARQUET_FOLDER = "reth_plain_accounts"  # Local folder containing parquet files


def get_parquet_files(folder_path: str) -> List[str]:
    """
    Get all parquet files from the specified folder.
    
    Args:
        folder_path: Path to the folder containing parquet files
        
    Returns:
        List of parquet file paths
    """
    folder = Path(folder_path)
    if not folder.exists():
        raise FileNotFoundError(f"Folder '{folder_path}' does not exist")
    
    parquet_files = sorted(folder.glob("*.parquet"))
    return [str(f.absolute()) for f in parquet_files]


def insert_parquet_data(parquet_file_path: str, batch_size: int = 1000000) -> tuple[bool, Optional[str], int]:
    """
    Read parquet file and insert data into ClickHouse.
    
    Args:
        parquet_file_path: Path to the parquet file to insert
        batch_size: Number of rows to insert per batch
        
    Returns:
        Tuple of (success: bool, error_message: Optional[str], row_count: int)
    """
    try:
        # Read parquet file
        df = pd.read_parquet(parquet_file_path)
        row_count = len(df)
        
        if row_count == 0:
            return True, None, 0
        
        # Insert data in batches
        table_name = TARGET_TABLE.split('.')[-1]  # Get table name without database
        
        for i in range(0, row_count, batch_size):
            batch_df = df.iloc[i:i+batch_size]
            client.insert_df(table_name, batch_df)
        
        return True, None, row_count
        
    except Exception as e:
        error_msg = str(e)
        return False, error_msg, 0


def execute_insert_with_retry(parquet_file_path: str, max_retries: int = 3) -> tuple[bool, Optional[str], int]:
    """
    Execute insert with retry logic for errors.
    
    Args:
        parquet_file_path: Path to the parquet file to insert
        max_retries: Maximum number of retry attempts
        
    Returns:
        Tuple of (success: bool, error_message: Optional[str], row_count: int)
    """
    retry_delay = 5  # seconds
    
    for attempt in range(1, max_retries + 1):
        try:
            success, error_msg, row_count = insert_parquet_data(parquet_file_path)
            
            if success:
                return True, None, row_count
            
            # Check if it's a retryable error (timeout or connection)
            is_retryable = any(keyword in error_msg.lower() for keyword in 
                             ['timeout', 'connect timed out', 'connection', 'poco_exception'])
            
            if attempt < max_retries and is_retryable:
                print(f"    Attempt {attempt}/{max_retries} failed: {error_msg[:100]}...")
                print(f"    Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                return False, error_msg, 0
                
        except Exception as e:
            error_msg = str(e)
            
            if attempt < max_retries:
                print(f"    Attempt {attempt}/{max_retries} failed: {error_msg[:100]}...")
                print(f"    Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                return False, error_msg, 0
    
    return False, "Max retries exceeded", 0


def execute_backfill(parquet_folder: str) -> None:
    """
    Execute the backfill operation by loading all parquet files from the folder.
    
    Args:
        parquet_folder: Path to folder containing parquet files
    """
    print(f"\nScanning folder: {parquet_folder}")
    
    try:
        parquet_files = get_parquet_files(parquet_folder)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return
    
    if not parquet_files:
        print(f"No parquet files found in '{parquet_folder}'")
        return
    
    total_files = len(parquet_files)
    files_processed = 0
    successful = 0
    failed = 0
    failed_files = []
    total_rows = 0
    
    print(f"Found {total_files} parquet files")
    print("=" * 80)
    
    backfill_start_time = time.time()
    
    for parquet_file in parquet_files:
        files_processed += 1
        progress = (files_processed / total_files) * 100
        
        file_name = Path(parquet_file).name
        print(f"[{files_processed}/{total_files}] Loading {file_name:<50} | Progress: {progress:>6.2f}%")
        
        chunk_start_time = time.time()
        success, error_msg, row_count = execute_insert_with_retry(parquet_file)
        chunk_end_time = time.time()
        
        chunk_time = chunk_end_time - chunk_start_time
        
        if success:
            successful += 1
            total_rows += row_count
            print(f"  ✓ Success ({row_count:,} rows, Time: {chunk_time:>6.2f}s)")
            # Small delay to avoid overwhelming the database
            time.sleep(0.5)
        else:
            failed += 1
            print(f"  ✗ Failed: {error_msg[:150]}")
            failed_files.append((file_name, error_msg))
    
    backfill_end_time = time.time()
    total_time = backfill_end_time - backfill_start_time
    
    # Print summary
    print("=" * 80)
    print(f"\nBackfill Summary")
    print("=" * 80)
    print(f"Total files: {total_files}")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Success rate: {(successful/total_files*100):.1f}%")
    print(f"Total rows inserted: {total_rows:,}")
    print(f"Total time: {total_time:.2f}s")
    if successful > 0:
        print(f"Average time per file: {total_time/successful:.2f}s")
        print(f"Average rows per second: {total_rows/total_time:,.0f}")
    
    # Print failed files
    if failed_files:
        print(f"\n{'=' * 80}")
        print(f"Failed Files")
        print(f"{'=' * 80}")
        for file_name, error in failed_files:
            print(f"\nFile: {file_name}")
            print(f"Error: {error[:200]}")


def check_table_exists() -> bool:
    """Check if the target table exists."""
    try:
        result = client.command(f"EXISTS TABLE {TARGET_TABLE}")
        return result == 1
    except Exception as e:
        print(f"Error checking if table exists: {e}")
        return False


def main():
    """Main backfill function."""
    print("=" * 80)
    print("Reth Plain Accounts Backfill")
    print("=" * 80)
    
    # Check if table exists
    if not check_table_exists():
        print(f"\nError: Table '{TARGET_TABLE}' does not exist.")
        print("Please create the table first using create_reth_tables.sql")
        return
    
    print(f"\nTable '{TARGET_TABLE}' exists.")
    
    # Check if parquet folder exists
    if not os.path.exists(PARQUET_FOLDER):
        print(f"\nError: Parquet folder '{PARQUET_FOLDER}' does not exist.")
        print(f"Please ensure the folder exists and contains .parquet files.")
        return
    
    print(f"\nConfiguration:")
    print(f"  Target table: {TARGET_TABLE}")
    print(f"  Parquet folder: {PARQUET_FOLDER}")
    
    # Optional: Prompt user for confirmation
    response = input("\nProceed with backfill? (yes/no): ")
    if response.lower() not in ['yes', 'y']:
        print("Backfill cancelled.")
        return
    
    # Execute backfill
    execute_backfill(PARQUET_FOLDER)


if __name__ == "__main__":
    main()


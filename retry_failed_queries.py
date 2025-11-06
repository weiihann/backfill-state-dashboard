"""
Script to re-execute failed queries from exception logs.

This script reads exception log files from the exceptions folder,
extracts failed queries, and re-executes them with retry logic.
"""

import os
import re
import time
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from typing import List, Tuple

load_dotenv()

# Database configuration
username = os.getenv('XATU_CLICKHOUSE_USERNAME')
password = os.getenv('XATU_CLICKHOUSE_PASSWORD')
url = os.getenv('XATU_CLICKHOUSE_URL')
protocol = os.getenv('XATU_CLICKHOUSE_PROTOCOL')

db_url = f"clickhouse+http://{username}:{password}@{url}/default?protocol={protocol}"
engine = create_engine(db_url)

# Configuration
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
DELAY_BETWEEN_QUERIES = 2  # seconds between successful queries


def parse_exception_log(log_file_path: str) -> List[str]:
    """
    Parse an exception log file and extract all failed queries.
    
    Args:
        log_file_path: Path to the exception log file
        
    Returns:
        List of SQL queries that failed
    """
    queries = []
    
    with open(log_file_path, 'r') as f:
        content = f.read()
    
    # Split by separator
    entries = content.split('--')
    
    for entry in entries:
        entry = entry.strip()
        if not entry:
            continue
            
        # Extract query using regex
        # Pattern: (query: <SQL_QUERY>)
        match = re.search(r'\(query:\s*(.+?)\)\s*$', entry, re.DOTALL | re.MULTILINE)
        if match:
            query = match.group(1).strip()
            queries.append(query)
    
    return queries


def execute_query_with_retry(query: str, max_retries: int = MAX_RETRIES) -> Tuple[bool, str]:
    """
    Execute a query with retry logic for timeout errors.
    
    Args:
        query: SQL query to execute
        max_retries: Maximum number of retry attempts
        
    Returns:
        Tuple of (success: bool, error_message: str)
    """
    for attempt in range(1, max_retries + 1):
        try:
            with engine.begin() as conn:
                conn.execute(text(query))
            return True, ""
        except Exception as e:
            error_msg = str(e)
            
            # Check if it's a timeout or connection error
            is_retryable = any(keyword in error_msg.lower() for keyword in 
                             ['timeout', 'connect timed out', 'connection', 'poco_exception'])
            
            if attempt < max_retries and is_retryable:
                print(f"  Attempt {attempt}/{max_retries} failed: {error_msg[:100]}...")
                print(f"  Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                return False, error_msg
    
    return False, "Max retries exceeded"


def process_exception_log(log_file_path: str, log_name: str) -> None:
    """
    Process an exception log file and re-execute all failed queries.
    
    Args:
        log_file_path: Path to the exception log file
        log_name: Name of the log for display purposes
    """
    print(f"\n{'=' * 80}")
    print(f"Processing: {log_name}")
    print(f"{'=' * 80}")
    
    # Parse queries from log
    queries = parse_exception_log(log_file_path)
    total_queries = len(queries)
    
    if total_queries == 0:
        print("No queries found in log file.")
        return
    
    print(f"Found {total_queries} failed queries to retry.\n")
    
    # Track results
    successful = 0
    failed = 0
    failed_queries = []
    
    start_time = time.time()
    
    # Execute each query
    for idx, query in enumerate(queries, 1):
        # Extract parquet file name for display
        match = re.search(r'/(\d+\.parquet)', query)
        file_name = match.group(1) if match else "unknown"
        
        print(f"[{idx}/{total_queries}] Executing query for: {file_name}")
        
        success, error_msg = execute_query_with_retry(query)
        
        if success:
            successful += 1
            print(f"  ✓ Success")
            # Add delay between successful queries to avoid overwhelming the server
            if idx < total_queries:
                time.sleep(DELAY_BETWEEN_QUERIES)
        else:
            failed += 1
            print(f"  ✗ Failed: {error_msg[:150]}")
            failed_queries.append((file_name, query, error_msg))
    
    end_time = time.time()
    total_time = end_time - start_time
    
    # Print summary
    print(f"\n{'=' * 80}")
    print(f"Summary for {log_name}")
    print(f"{'=' * 80}")
    print(f"Total queries: {total_queries}")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Success rate: {(successful/total_queries*100):.1f}%")
    print(f"Total time: {total_time:.2f}s")
    
    # Print failed queries details
    if failed_queries:
        print(f"\n{'=' * 80}")
        print(f"Failed Queries Details")
        print(f"{'=' * 80}")
        for file_name, query, error in failed_queries:
            print(f"\nFile: {file_name}")
            print(f"Error: {error[:200]}")
            print(f"Query: {query[:150]}...")


def main():
    """Main function to process all exception logs."""
    print("=" * 80)
    print("Retry Failed Queries Script")
    print("=" * 80)
    
    exceptions_dir = "exceptions"
    
    # Check if exceptions directory exists
    if not os.path.exists(exceptions_dir):
        print(f"Error: '{exceptions_dir}' directory not found.")
        return
    
    # Get all exception log files
    log_files = []
    for item in os.listdir(exceptions_dir):
        item_path = os.path.join(exceptions_dir, item)
        if os.path.isfile(item_path):
            log_files.append((item_path, item))
    
    if not log_files:
        print(f"No exception log files found in '{exceptions_dir}' directory.")
        return
    
    print(f"\nFound {len(log_files)} exception log file(s):")
    for _, name in log_files:
        print(f"  - {name}")
    
    # Process each log file
    overall_start = time.time()
    
    for log_path, log_name in log_files:
        process_exception_log(log_path, log_name)
    
    overall_end = time.time()
    overall_time = overall_end - overall_start
    
    print(f"\n{'=' * 80}")
    print(f"All exception logs processed!")
    print(f"Total execution time: {overall_time:.2f}s")
    print(f"{'=' * 80}")


if __name__ == "__main__":
    main()


"""Base backfiller class with common functionality."""

import time
import logging
from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
from sqlalchemy import text
from core.utils import get_block_range

logger = logging.getLogger(__name__)


class BaseBackfiller(ABC):
    """Base class for all backfill operations."""
    
    def __init__(self, source_engine, target_engine, step_size: int = 10000):
        """
        Initialize the backfiller.
        
        Args:
            source_engine: SQLAlchemy engine for source database (read)
            target_engine: SQLAlchemy engine for target database (write)
            step_size: Number of blocks to process in each chunk
        """
        self.source_engine = source_engine
        self.target_engine = target_engine
        self.step_size = step_size
        
    @property
    @abstractmethod
    def table_name(self) -> str:
        """Return the name of the table to backfill."""
        pass
    
    @property
    @abstractmethod
    def source_tables(self) -> List[str]:
        """Return list of source tables required for this backfill."""
        pass
    
    @property
    @abstractmethod
    def description(self) -> str:
        """Return a description of what this backfiller does."""
        pass
    
    @abstractmethod
    def generate_sql(self, start_block: int, end_block: int) -> str:
        """
        Generate the SQL query for backfilling a block range.
        
        Args:
            start_block: Starting block number
            end_block: Ending block number
            
        Returns:
            SQL query string
        """
        pass
    
    def get_additional_info(self) -> Dict[str, Any]:
        """
        Return any additional configuration info for display.
        Can be overridden by subclasses.
        """
        return {}
    
    def execute_chunk(self, start_block: int, end_block: int) -> float:
        """
        Execute backfill for a single chunk of blocks.
        
        Args:
            start_block: Starting block number for this chunk
            end_block: Ending block number for this chunk
            
        Returns:
            Time taken in seconds
        """
        start_time = time.time()
        sql = self.generate_sql(start_block, end_block)
        
        if sql:  # Only execute if we have a valid query
            with self.target_engine.begin() as conn:
                conn.execute(text(sql))
        
        return time.time() - start_time
    
    def execute(self, start_block: Optional[int] = None, end_block: Optional[int] = None):
        """
        Execute the complete backfill operation.
        
        Args:
            start_block: Override starting block number (optional)
            end_block: Override ending block number (optional)
        """
        print("=" * 80)
        print(f"{self.description}")
        print("=" * 80)
        
        # Determine block range if not provided
        if start_block is None or end_block is None:
            calc_start, calc_end = get_block_range(
                self.table_name, 
                self.source_tables, 
                self.source_engine, 
                self.target_engine
            )
            if start_block is None:
                start_block = calc_start
            if end_block is None:
                end_block = calc_end
        
        # Display configuration
        print(f"\nConfiguration:")
        print(f"  Target table: {self.table_name}")
        print(f"  Source tables:")
        for table in self.source_tables:
            print(f"    - {table}")
        print(f"  Step size: {self.step_size}")
        
        # Display any additional info
        additional_info = self.get_additional_info()
        for key, value in additional_info.items():
            print(f"  {key}: {value}")
        
        # Check if there's work to do
        if end_block <= start_block:
            print(f"\nNo blocks to process (end_block: {end_block} <= start_block: {start_block})")
            return
        
        total_blocks = end_block - start_block + 1
        blocks_processed = 0
        
        print(f"\nProcessing {total_blocks} blocks in steps of {self.step_size}")
        print(f"Block range: {start_block} to {end_block}")
        print("=" * 80)
        
        backfill_start_time = time.time()
        
        # Process in chunks
        for lower in range(start_block, end_block + 1, self.step_size):
            upper = min(lower + self.step_size - 1, end_block)
            
            try:
                chunk_time = self.execute_chunk(lower, upper)
                
                blocks_processed += (upper - lower + 1)
                progress = (blocks_processed / total_blocks) * 100
                
                # Allow subclasses to add notes about specific block ranges
                note = self.get_block_range_note(lower, upper)
                
                print(f"Processed blocks {lower:>10} - {upper:>10}{note} | "
                      f"Progress: {progress:>6.2f}% | "
                      f"Time: {chunk_time:>6.2f}s")
                      
            except Exception as e:
                logger.error(f"Error processing blocks {lower}-{upper}: {e}")
                raise
        
        # Final statistics
        backfill_end_time = time.time()
        total_time = backfill_end_time - backfill_start_time
        
        print("=" * 80)
        print(f"\nBackfill completed!")
        print(f"  Total blocks processed: {blocks_processed}")
        print(f"  Total time: {total_time:.2f}s")
        if total_time > 0:
            print(f"  Average speed: {blocks_processed/total_time:.2f} blocks/sec")
    
    def get_block_range_note(self, start_block: int, end_block: int) -> str:
        """
        Get a note about a specific block range (e.g., for EIP markers).
        Can be overridden by subclasses.
        
        Args:
            start_block: Starting block of the range
            end_block: Ending block of the range
            
        Returns:
            A string note (empty if nothing special about this range)
        """
        return ""

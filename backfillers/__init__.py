"""Backfiller implementations for each table."""

from typing import Dict, Any
from core.backfiller import BaseBackfiller
from config.table_definitions import TABLE_CONFIGS

# Import individual backfillers - 1 file per table
from backfillers.address_diffs import AddressDiffsBackfiller
from backfillers.address_reads import AddressReadsBackfiller
from backfillers.address_first_access import AddressFirstAccessBackfiller
from backfillers.address_last_access import AddressLastAccessBackfiller
from backfillers.address_slots_stat import AddressSlotsStatBackfiller
from backfillers.address_storage_slot_first_access import AddressStorageSlotFirstAccessBackfiller
from backfillers.address_storage_slot_last_access import AddressStorageSlotLastAccessBackfiller
from backfillers.block_slots_stat import BlockSlotsStatBackfiller
from backfillers.pre_6780_accounts_destructs import Pre6780AccountsDestructsBackfiller
from backfillers.post_6780_accounts_destructs import Post6780AccountsDestructsBackfiller
from backfillers.accounts_alive import AccountsAliveBackfiller

# Registry of available backfillers
BACKFILLER_REGISTRY = {
    'address_diffs': AddressDiffsBackfiller,
    'address_reads': AddressReadsBackfiller,
    'address_first_access': AddressFirstAccessBackfiller,
    'address_last_access': AddressLastAccessBackfiller,
    'address_storage_slot_first_access': AddressStorageSlotFirstAccessBackfiller,
    'address_storage_slot_last_access': AddressStorageSlotLastAccessBackfiller,
    'address_slots_stat_per_block': AddressSlotsStatBackfiller,
    'block_slots_stat': BlockSlotsStatBackfiller,
    'pre_6780_accounts_destructs': Pre6780AccountsDestructsBackfiller,
    'post_6780_accounts_destructs': Post6780AccountsDestructsBackfiller,
    'accounts_alive': AccountsAliveBackfiller,
}


def get_backfiller(table_key: str, source_engine, target_engine, step_size: int = 10000) -> BaseBackfiller:
    """
    Get the appropriate backfiller for a given table.
    
    Args:
        table_key: Key identifying the table
        source_engine: SQLAlchemy engine for source database
        target_engine: SQLAlchemy engine for target database
        step_size: Number of blocks to process per chunk
        
    Returns:
        An instance of the appropriate backfiller
        
    Raises:
        ValueError: If table_key is not recognized
    """
    if table_key not in BACKFILLER_REGISTRY:
        # For tables not yet migrated, use a generic backfiller
        # This allows gradual migration of existing scripts
        return GenericBackfiller(table_key, source_engine, target_engine, step_size)
    
    backfiller_class = BACKFILLER_REGISTRY[table_key]
    return backfiller_class(source_engine, target_engine, step_size)


class GenericBackfiller(BaseBackfiller):
    """Generic backfiller for tables not yet fully migrated."""
    
    def __init__(self, table_key: str, source_engine, target_engine, step_size: int = 10000):
        super().__init__(source_engine, target_engine, step_size)
        if table_key not in TABLE_CONFIGS:
            raise ValueError(f"Unknown table: {table_key}")
        self.config = TABLE_CONFIGS[table_key]
        
    @property
    def table_name(self) -> str:
        return self.config['name']
    
    @property
    def source_tables(self) -> list:
        return self.config['source_tables']
    
    @property
    def description(self) -> str:
        return self.config['description']
    
    def generate_sql(self, start_block: int, end_block: int) -> str:
        """Generic SQL generation - to be overridden by specific implementations."""
        # This would need to be implemented for each specific table
        # For now, we'll raise an error
        raise NotImplementedError(f"SQL generation not yet implemented for {self.table_name}")

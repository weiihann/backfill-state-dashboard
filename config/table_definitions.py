"""Table definitions and configurations for all backfillable tables."""

from typing import Dict, Any

# Complete table registry with all configurations
TABLE_CONFIGS: Dict[str, Any] = {
    'address_diffs': {
        'name': 'mainnet.int_address_diffs',
        'description': 'Aggregates diff data from balance, storage, nonce diffs and contracts',
        'source_tables': [
            'canonical_execution_balance_diffs',
            'canonical_execution_storage_diffs',
            'canonical_execution_nonce_diffs',
            'canonical_execution_contracts',
            'canonical_execution_transaction',
        ]
    },
    
    'address_reads': {
        'name': 'mainnet.int_address_reads',
        'description': 'Tracks read operations for addresses',
        'source_tables': [
            'canonical_execution_balance_reads',
            'canonical_execution_nonce_reads',
            'canonical_execution_storage_reads',
            'canonical_execution_transaction',
        ]
    },
    
    'address_first_access': {
        'name': 'mainnet.int_address_first_access',
        'description': 'Tracks the first access block for each address',
        'source_tables': [
            'canonical_execution_balance_diffs',
            'canonical_execution_balance_reads',
            'canonical_execution_contracts',
            'canonical_execution_nonce_reads',
            'canonical_execution_nonce_diffs',
            'canonical_execution_storage_diffs',
            'canonical_execution_storage_reads',
        ]
    },
    
    'address_last_access': {
        'name': 'mainnet.int_address_last_access',
        'description': 'Tracks the last access block for each address',
        'source_tables': [
            'canonical_execution_balance_diffs',
            'canonical_execution_balance_reads',
            'canonical_execution_contracts',
            'canonical_execution_nonce_reads',
            'canonical_execution_nonce_diffs',
            'canonical_execution_storage_diffs',
            'canonical_execution_storage_reads',
        ]
    },
    
    'address_storage_slot_first_access': {
        'name': 'mainnet.int_address_storage_slot_first_access',
        'description': 'Tracks first access to storage slots',
        'source_tables': [
            'canonical_execution_storage_diffs',
            'canonical_execution_storage_reads',
            'canonical_execution_transaction',
        ]
    },
    
    'address_storage_slot_last_access': {
        'name': 'mainnet.int_address_storage_slot_last_access',
        'description': 'Tracks last access to storage slots',
        'source_tables': [
            'canonical_execution_storage_diffs',
            'canonical_execution_storage_reads',
            'canonical_execution_transaction',
        ]
    },
    
    'address_slots_stat_per_block': {
        'name': 'mainnet.int_address_slots_stat_per_block',
        'description': 'Analyzes storage slot statistics per address per block',
        'source_tables': [
            'canonical_execution_storage_diffs',
            'canonical_execution_transaction',
        ]
    },
    
    'block_slots_stat': {
        'name': 'mainnet.int_block_slots_stat',
        'description': 'Aggregates storage slot statistics per block',
        'source_tables': [
            'canonical_execution_storage_diffs',
            'canonical_execution_transaction',
        ]
    },
    
    'pre_6780_accounts_destructs': {
        'name': 'mainnet.int_pre_6780_accounts_destructs',
        'description': 'Tracks self-destruct operations before EIP-6780',
        'source_tables': [
            'canonical_execution_traces',
            'canonical_execution_transaction',
        ]
    },
    
    'post_6780_accounts_destructs': {
        'name': 'mainnet.int_post_6780_accounts_destructs',
        'description': 'Tracks self-destruct operations after EIP-6780',
        'source_tables': [
            'canonical_execution_traces',
            'canonical_execution_contracts',
            'canonical_execution_transaction',
        ]
    },
    
    'accounts_alive': {
        'name': 'mainnet.int_accounts_alive',
        'description': 'Determines account alive status based on diffs and destructs',
        'source_tables': [
            'mainnet.int_address_diffs',
            'mainnet.int_post_6780_accounts_destructs',
            'mainnet.int_pre_6780_accounts_destructs',
        ]
    },
}

def get_table_config(table_key: str) -> Dict[str, Any]:
    """Get configuration for a specific table."""
    if table_key not in TABLE_CONFIGS:
        raise ValueError(f"Unknown table: {table_key}")
    return TABLE_CONFIGS[table_key]

def list_available_tables() -> list:
    """List all available table keys."""
    return list(TABLE_CONFIGS.keys())

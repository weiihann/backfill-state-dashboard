"""Database configuration and connection management."""

import os
from typing import Tuple
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()


class DatabaseConfig:
    """Manages database configurations for source and target databases."""
    
    def __init__(self):
        # Source database (for reading)
        self.source_username = os.getenv('SOURCE_CLICKHOUSE_USERNAME', 'default')
        self.source_password = os.getenv('SOURCE_CLICKHOUSE_PASSWORD', '')
        self.source_url = os.getenv('SOURCE_CLICKHOUSE_URL', 'localhost')
        self.source_protocol = os.getenv('SOURCE_CLICKHOUSE_PROTOCOL', 'http')
        
        # Target database (for writing)
        self.target_username = os.getenv('TARGET_CLICKHOUSE_USERNAME', 'default')
        self.target_password = os.getenv('TARGET_CLICKHOUSE_PASSWORD', '')
        self.target_url = os.getenv('TARGET_CLICKHOUSE_URL', 'localhost')
        self.target_protocol = os.getenv('TARGET_CLICKHOUSE_PROTOCOL', 'http')
        
        # Legacy support for single database config
        if not os.getenv('SOURCE_CLICKHOUSE_URL') and os.getenv('XATU_CLICKHOUSE_URL'):
            print("Warning: Using legacy XATU_CLICKHOUSE_* environment variables.")
            print("Please update to SOURCE_CLICKHOUSE_* and TARGET_CLICKHOUSE_* variables.")
            
            legacy_username = os.getenv('XATU_CLICKHOUSE_USERNAME', 'default')
            legacy_password = os.getenv('XATU_CLICKHOUSE_PASSWORD', '')
            legacy_url = os.getenv('XATU_CLICKHOUSE_URL', 'localhost')
            legacy_protocol = os.getenv('XATU_CLICKHOUSE_PROTOCOL', 'http')
            
            # Use legacy config for both source and target
            self.source_username = legacy_username
            self.source_password = legacy_password
            self.source_url = legacy_url
            self.source_protocol = legacy_protocol
            
            self.target_username = legacy_username
            self.target_password = legacy_password
            self.target_url = legacy_url
            self.target_protocol = legacy_protocol
    
    def get_source_engine(self):
        """Create and return a SQLAlchemy engine for the source database."""
        db_url = (
            f"clickhouse+http://{self.source_username}:{self.source_password}"
            f"@{self.source_url}/default?protocol={self.source_protocol}"
        )
        return create_engine(db_url)
    
    def get_target_engine(self):
        """Create and return a SQLAlchemy engine for the target database."""
        db_url = (
            f"clickhouse+http://{self.target_username}:{self.target_password}"
            f"@{self.target_url}/default?protocol={self.target_protocol}"
        )
        return create_engine(db_url)
    
    def get_engines(self) -> Tuple:
        """Return both source and target engines."""
        return self.get_source_engine(), self.get_target_engine()

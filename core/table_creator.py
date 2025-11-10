"""Table creation functionality."""

from typing import Dict, List
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)


class TableCreator:
    """Creates ClickHouse tables from SQL definitions."""
    
    def __init__(self, target_engine):
        """
        Initialize the table creator.
        
        Args:
            target_engine: SQLAlchemy engine for target database
        """
        self.target_engine = target_engine
    
    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists.
        
        Args:
            table_name: Name of the table (can include schema)
            
        Returns:
            True if table exists, False otherwise
        """
        # Parse schema and table name
        if '.' in table_name:
            schema, table = table_name.split('.', 1)
        else:
            schema = 'default'
            table = table_name
            
        query = """
        SELECT 1
        FROM system.tables
        WHERE database = :schema AND name = :table
        """
        
        try:
            with self.target_engine.begin() as conn:
                result = conn.execute(
                    text(query),
                    {'schema': schema, 'table': table}
                )
                return result.fetchone() is not None
        except Exception as e:
            logger.error(f"Error checking if table {table_name} exists: {e}")
            return False
    
    def create_tables(self, table_sqls: Dict[str, List[str]], check_exists: bool = True):
        """
        Create tables from SQL definitions.
        
        Args:
            table_sqls: Dictionary mapping table names to list of CREATE statements
                       (local and distributed table creation)
            check_exists: Whether to check if table exists before creating
        """
        for table_name, sql_statements in table_sqls.items():
            # Check if table already exists
            if check_exists and self.table_exists(table_name):
                print(f"✓ Table {table_name} already exists")
                continue
            
            print(f"Creating table {table_name}...")
            
            try:
                with self.target_engine.begin() as conn:
                    for sql in sql_statements:
                        if sql.strip():
                            conn.execute(text(sql))
                print(f"✓ Successfully created {table_name}")
                
            except Exception as e:
                if "already exists" in str(e).lower():
                    print(f"✓ Table {table_name} already exists")
                else:
                    logger.error(f"Error creating table {table_name}: {e}")
                    raise
    
    def create_schema_if_not_exists(self, schema_name: str = 'mainnet'):
        """
        Create a schema/database if it doesn't exist.
        
        Args:
            schema_name: Name of the schema to create
        """
        try:
            with self.target_engine.begin() as conn:
                conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {schema_name}"))
                print(f"✓ Schema {schema_name} is ready")
        except Exception as e:
            logger.error(f"Error creating schema {schema_name}: {e}")
            raise

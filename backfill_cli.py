#!/usr/bin/env python3
"""
Main CLI interface for backfilling state dashboard tables.

Usage:
    python backfill_cli.py list                           # List available tables
    python backfill_cli.py run --tables address_diffs     # Backfill specific table
    python backfill_cli.py run --all                      # Backfill all tables
    python backfill_cli.py create-tables --tables address_diffs  # Create specific tables
"""

import click
import logging
import sys
from typing import List, Optional
from tabulate import tabulate
from colorama import init, Fore, Style

# Initialize colorama for cross-platform colored output
init(autoreset=True)

# Add current directory to path for imports
sys.path.insert(0, '.')

from config.database import DatabaseConfig
from config.table_definitions import TABLE_CONFIGS, list_available_tables
from core.table_creator import TableCreator
from backfillers import get_backfiller
from config.table_schemas import TABLE_SCHEMAS

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@click.group()
def cli():
    """Backfill State Dashboard CLI - Manage and execute table backfills."""
    pass


@cli.command()
def list():
    """List all available tables for backfilling."""
    click.echo(f"\n{Fore.CYAN}{'='*80}{Style.RESET_ALL}")
    click.echo(f"{Fore.CYAN}Available Tables for Backfilling{Style.RESET_ALL}")
    click.echo(f"{Fore.CYAN}{'='*80}{Style.RESET_ALL}\n")
    
    table_data = []
    for key, config in TABLE_CONFIGS.items():
        table_data.append([
            key,
            config['name'],
            config['description'][:50] + '...' if len(config['description']) > 50 else config['description']
        ])
    
    headers = ['Key', 'Table Name', 'Description']
    click.echo(tabulate(table_data, headers=headers, tablefmt='grid'))
    
    click.echo(f"\n{Fore.GREEN}Usage examples:{Style.RESET_ALL}")
    click.echo("  python backfill_cli.py run --tables address_diffs")
    click.echo("  python backfill_cli.py run --tables address_diffs,accounts_alive")
    click.echo("  python backfill_cli.py run --all")
    click.echo()


@cli.command()
@click.option('--tables', '-t', help='Comma-separated list of table keys to backfill')
@click.option('--all', 'backfill_all', is_flag=True, help='Backfill all available tables')
@click.option('--start-block', type=int, help='Override start block number')
@click.option('--end-block', type=int, help='Override end block number')
@click.option('--step-size', type=int, default=10000, help='Number of blocks per chunk (default: 10000)')
@click.option('--create-tables', is_flag=True, help='Create tables if they don\'t exist')
def run(tables: Optional[str], backfill_all: bool, start_block: Optional[int], 
        end_block: Optional[int], step_size: int, create_tables: bool):
    """Execute backfill operations for specified tables."""
    
    # Determine which tables to backfill
    if backfill_all:
        table_keys = list_available_tables()
    elif tables:
        table_keys = [t.strip() for t in tables.split(',')]
    else:
        click.echo(f"{Fore.RED}Error: Please specify --tables or --all{Style.RESET_ALL}")
        return
    
    # Validate table keys
    available_tables = list_available_tables()
    invalid_tables = [t for t in table_keys if t not in available_tables]
    if invalid_tables:
        click.echo(f"{Fore.RED}Error: Unknown tables: {', '.join(invalid_tables)}{Style.RESET_ALL}")
        click.echo(f"Use 'python backfill_cli.py list' to see available tables")
        return
    
    # Initialize database connections
    click.echo(f"\n{Fore.CYAN}Initializing database connections...{Style.RESET_ALL}")
    db_config = DatabaseConfig()
    source_engine = db_config.get_source_engine()
    target_engine = db_config.get_target_engine()
    
    # Create tables if requested
    if create_tables:
        click.echo(f"\n{Fore.CYAN}Creating tables if they don't exist...{Style.RESET_ALL}")
        creator = TableCreator(target_engine)
        creator.create_schema_if_not_exists('mainnet')
        
        # Get all table schemas
        all_schemas = {}
        all_schemas.update(TABLE_SCHEMAS)
        
        # Create tables for the selected keys
        tables_to_create = {k: all_schemas[k] for k in table_keys if k in all_schemas}
        if tables_to_create:
            creator.create_tables(tables_to_create)
        else:
            click.echo(f"{Fore.YELLOW}No table schemas found for selected tables{Style.RESET_ALL}")
    
    # Execute backfills
    click.echo(f"\n{Fore.CYAN}Starting backfill operations...{Style.RESET_ALL}")
    click.echo(f"Tables to backfill: {', '.join(table_keys)}")
    
    for table_key in table_keys:
        click.echo(f"\n{Fore.GREEN}{'='*80}{Style.RESET_ALL}")
        click.echo(f"{Fore.GREEN}Backfilling: {table_key}{Style.RESET_ALL}")
        click.echo(f"{Fore.GREEN}{'='*80}{Style.RESET_ALL}")
        
        try:
            # Get the appropriate backfiller
            backfiller = get_backfiller(
                table_key,
                source_engine,
                target_engine,
                step_size
            )
            
            # Execute the backfill
            backfiller.execute(start_block, end_block)
            
            click.echo(f"{Fore.GREEN}✓ Successfully completed backfill for {table_key}{Style.RESET_ALL}")
            
        except Exception as e:
            click.echo(f"{Fore.RED}✗ Error backfilling {table_key}: {e}{Style.RESET_ALL}")
            logger.exception(f"Error backfilling {table_key}")
            continue
    
    click.echo(f"\n{Fore.CYAN}Backfill operations completed!{Style.RESET_ALL}")


@cli.command()
@click.option('--tables', '-t', help='Comma-separated list of table keys to create')
@click.option('--all', 'create_all', is_flag=True, help='Create all available tables')
def create_tables(tables: Optional[str], create_all: bool):
    """Create tables without backfilling data."""
    
    # Determine which tables to create
    if create_all:
        table_keys = list_available_tables()
    elif tables:
        table_keys = [t.strip() for t in tables.split(',')]
    else:
        click.echo(f"{Fore.RED}Error: Please specify --tables or --all{Style.RESET_ALL}")
        return
    
    # Initialize database connection
    click.echo(f"\n{Fore.CYAN}Initializing database connection...{Style.RESET_ALL}")
    db_config = DatabaseConfig()
    target_engine = db_config.get_target_engine()
    
    # Create tables
    click.echo(f"\n{Fore.CYAN}Creating tables...{Style.RESET_ALL}")
    creator = TableCreator(target_engine)
    creator.create_schema_if_not_exists('mainnet')
    
    # Get all table schemas
    all_schemas = {}
    all_schemas.update(TABLE_SCHEMAS)
    
    for table_key in table_keys:
        config = TABLE_CONFIGS.get(table_key)
        if config and table_key in all_schemas:
            click.echo(f"Creating {config['name']}...")
            tables_to_create = {config['name']: all_schemas[table_key]}
            creator.create_tables(tables_to_create)
        else:
            click.echo(f"{Fore.YELLOW}No schema found for table: {table_key}{Style.RESET_ALL}")
    
    click.echo(f"\n{Fore.GREEN}Table creation completed!{Style.RESET_ALL}")


@cli.command()
def info():
    """Display information about the current configuration."""
    click.echo(f"\n{Fore.CYAN}{'='*80}{Style.RESET_ALL}")
    click.echo(f"{Fore.CYAN}Backfill State Dashboard - Configuration Info{Style.RESET_ALL}")
    click.echo(f"{Fore.CYAN}{'='*80}{Style.RESET_ALL}\n")
    
    db_config = DatabaseConfig()
    
    click.echo(f"{Fore.GREEN}Source Database (Read):{Style.RESET_ALL}")
    click.echo(f"  URL: {db_config.source_url}")
    click.echo(f"  Username: {db_config.source_username}")
    click.echo(f"  Protocol: {db_config.source_protocol}")
    
    click.echo(f"\n{Fore.GREEN}Target Database (Write):{Style.RESET_ALL}")
    click.echo(f"  URL: {db_config.target_url}")
    click.echo(f"  Username: {db_config.target_username}")
    click.echo(f"  Protocol: {db_config.target_protocol}")
    
    click.echo(f"\n{Fore.GREEN}Available Tables:{Style.RESET_ALL}")
    click.echo(f"  Total: {len(TABLE_CONFIGS)} tables")
    click.echo()


if __name__ == '__main__':
    cli()

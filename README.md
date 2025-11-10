# Backfill Scripts for xatu-cbt

## Prerequisites

- Python 3.8 or higher
- Access to source ClickHouse database (read permissions)
- Access to target ClickHouse database (write permissions)
- Network connectivity to both databases
- Setup local clickhouse using [Xatu](https://github.com/ethpandaops/xatu?tab=readme-ov-file#local-clickhouse)

## Installation

1. **Install dependencies**

```bash
pip install -r requirements.txt
```

2. **Set up environment variables**

Create a `.env` file in the project root:

```bash
# Source Database (Read)
SOURCE_CLICKHOUSE_USERNAME=default
SOURCE_CLICKHOUSE_PASSWORD=your_password
SOURCE_CLICKHOUSE_URL=source-host:8123
SOURCE_CLICKHOUSE_PROTOCOL=http

# Target Database (Write)
TARGET_CLICKHOUSE_USERNAME=default
TARGET_CLICKHOUSE_PASSWORD=your_password
TARGET_CLICKHOUSE_URL=target-host:8123
TARGET_CLICKHOUSE_PROTOCOL=http
```

**Note**: Legacy `XATU_CLICKHOUSE_*` environment variables are still supported for backward compatibility.

## Configuration

### Database Configuration

The application uses environment variables to configure database connections:

| Variable | Description | Default |
|----------|-------------|---------|
| `SOURCE_CLICKHOUSE_USERNAME` | Source database username | `default` |
| `SOURCE_CLICKHOUSE_PASSWORD` | Source database password | _(empty)_ |
| `SOURCE_CLICKHOUSE_URL` | Source database URL | `localhost` |
| `SOURCE_CLICKHOUSE_PROTOCOL` | Source database protocol | `http` |
| `TARGET_CLICKHOUSE_USERNAME` | Target database username | `default` |
| `TARGET_CLICKHOUSE_PASSWORD` | Target database password | _(empty)_ |
| `TARGET_CLICKHOUSE_URL` | Target database URL | `localhost` |
| `TARGET_CLICKHOUSE_PROTOCOL` | Target database protocol | `http` |

## Usage

### Basic Commands

#### List Available Tables

```bash
python3 backfill_cli.py list
```

Displays all available tables that can be backfilled with their descriptions.

#### Display Configuration Info

```bash
python3 backfill_cli.py info
```

Shows current database configuration and connection details.

#### Create Tables

Create specific tables:

```bash
python3 backfill_cli.py create-tables --tables address_diffs
```

Create all tables:

```bash
python3 backfill_cli.py create-tables --all
```

#### Run Backfill Operations

Backfill a single table:

```bash
python3 backfill_cli.py run --tables address_diffs
```

Backfill multiple tables:

```bash
python3 backfill_cli.py run --tables address_diffs,accounts_alive
```

Backfill all tables:

```bash
python3 backfill_cli.py run --all
```

### Advanced Usage

#### Custom Block Range

```bash
python3 backfill_cli.py run --tables address_diffs \
  --start-block 1000000 \
  --end-block 2000000 \
  --step-size 5000
```

#### Create Tables and Backfill

```bash
python3 backfill_cli.py run --tables address_diffs \
  --create-tables
```

This will automatically create the table if it doesn't exist before backfilling.

## Available Tables

The following tables can be backfilled:

| Key | Table Name | Description |
|-----|------------|-------------|
| `accounts_alive` | Account lifecycle tracking | Tracks account creation and destruction |
| `address_diffs` | Address state changes | Records state changes for addresses |
| `address_first_access` | First access tracking | Records first access time for addresses |
| `address_last_access` | Last access tracking | Records last access time for addresses |
| `address_reads` | Address read operations | Tracks read operations on addresses |
| `address_slots_stat` | Storage slot statistics | Statistics per address per block |
| `address_storage_slot_first_access` | Storage slot first access | First access to storage slots |
| `address_storage_slot_last_access` | Storage slot last access | Last access to storage slots |
| `block_slots_stat` | Block-level statistics | Aggregate statistics per block |
| `pre_6780_accounts_destructs` | Pre-EIP-6780 destructions | Account destructions before EIP-6780 |
| `post_6780_accounts_destructs` | Post-EIP-6780 destructions | Account destructions after EIP-6780 |

## Architecture

### Project Structure

```
backfill-state-dashboard/
├── backfill_cli.py           # Main CLI entry point
├── backfillers/              # Individual backfiller modules
│   ├── __init__.py
│   ├── accounts_alive.py
│   ├── address_diffs.py
│   └── ...
├── config/                   # Configuration modules
│   ├── database.py          # Database connection management
│   ├── table_definitions.py # Table metadata
│   └── table_schemas.py     # SQL table schemas
├── core/                     # Core functionality
│   ├── backfiller.py        # Base backfiller class
│   ├── table_creator.py     # Table creation logic
│   └── utils.py             # Utility functions
└── requirements.txt         # Python dependencies
```

### How It Works

1. **Database Connection**: Establishes connections to source (read) and target (write) databases
2. **Table Discovery**: Identifies available tables and their configurations
3. **Block Range Calculation**: Determines which blocks need to be processed
4. **Chunked Processing**: Processes data in configurable block ranges (default: 10,000 blocks)
5. **Data Transformation**: Executes SQL transformations specific to each table type
6. **Progress Tracking**: Reports progress with timing and performance metrics
7. **Error Handling**: Catches and logs errors, allowing partial success

### Base Backfiller Class

All backfillers inherit from `BaseBackfiller`, which provides:

- Standardized initialization with source/target engines
- Chunked execution with progress tracking
- Error handling and logging
- Performance metrics (blocks/second)
- Block range calculation

Each specific backfiller implements:

- `table_name`: Target table name
- `source_tables`: List of source tables needed
- `description`: Human-readable description
- `generate_sql()`: SQL query generation for the backfill

## Development

### Adding a New Backfiller

1. Create a new file in `backfillers/` directory (e.g., `my_new_table.py`)

```python
from core.backfiller import BaseBackfiller

class MyNewTableBackfiller(BaseBackfiller):
    @property
    def table_name(self) -> str:
        return "mainnet.my_new_table"
    
    @property
    def source_tables(self) -> List[str]:
        return ["mainnet.source_table"]
    
    @property
    def description(self) -> str:
        return "Backfills my new table"
    
    def generate_sql(self, start_block: int, end_block: int) -> str:
        return f"""
            INSERT INTO {self.table_name}
            SELECT * FROM mainnet.source_table
            WHERE block_number >= {start_block}
            AND block_number <= {end_block}
        """
```

2. Register it in `backfillers/__init__.py`

3. Add table configuration in `config/table_definitions.py`

4. Add table schema in `config/table_schemas.py`

### Running Tests

```bash
# Run with verbose output to see detailed logging
python3 backfill_cli.py run --tables my_new_table --start-block 1 --end-block 100
```

## Troubleshooting

### Connection Issues

**Problem**: Cannot connect to database

**Solutions**:
- Verify database URLs and credentials in `.env` file
- Check network connectivity to database hosts
- Ensure protocol (http/https) matches database configuration
- Verify firewall rules allow connections

### Performance Issues

**Problem**: Backfill is running slowly

**Solutions**:
- Reduce `--step-size` to process smaller chunks
- Check database performance and query load
- Verify network latency between client and databases
- Consider running backfills during off-peak hours

### Memory Issues

**Problem**: Out of memory errors

**Solutions**:
- Decrease `--step-size` to reduce memory footprint
- Process tables one at a time instead of using `--all`
- Monitor system resources during execution

### Block Range Issues

**Problem**: No blocks to process message

**Solutions**:
- Check if target table already contains the data
- Verify source tables contain data in the expected range
- Use `--start-block` and `--end-block` to override automatic detection

### Missing Tables

**Problem**: Table doesn't exist error

**Solutions**:
- Use `--create-tables` flag to auto-create tables
- Or run `python3 backfill_cli.py create-tables --tables <table_name>` first
- Verify database permissions allow table creation

### Code Style

- Follow PEP 8 style guidelines
- Use type hints where appropriate
- Add docstrings to all classes and functions
- Keep functions focused and single-purpose

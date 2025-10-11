# ry-pg-utils

A Python utility library for PostgreSQL database operations with dynamic table creation, connection management, and Protocol Buffer integration.

## Overview

`ry-pg-utils` provides a robust framework for working with PostgreSQL databases in Python applications. It includes utilities for:

- Database connection management with connection pooling
- Dynamic table creation from Protocol Buffer message definitions
- Thread-safe session management
- Multi-backend support with automatic backend ID tracking
- Database updater for dynamic configuration
- Argument parsing for PostgreSQL connection parameters

## Features

- **Connection Management**: Thread-safe PostgreSQL connection pooling with automatic retry logic
- **Dynamic Tables**: Automatically create and manage database tables from Protocol Buffer message schemas
- **Multi-Backend Support**: Track data across multiple backend instances with automatic ID tagging
- **Session Management**: Context managers for safe database session handling
- **Configuration System**: Flexible configuration via environment variables and runtime settings
- **Type Safety**: Full type hints and mypy support

## Installation

```bash
pip install ry-pg-utils
```

### Dependencies

- Python 3.12+
- PostgreSQL database
- SQLAlchemy
- Protocol Buffer support

## Configuration

`ry-pg-utils` uses a flexible configuration system that can be customized in multiple ways:

### 1. Environment Variables

Create a `.env` file in your project root:

```bash
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=mydb
POSTGRES_USER=postgres
POSTGRES_PASSWORD=secret
```

### 2. Configuration Object

```python
from ry_pg_utils.config import pg_config

# Access configuration
print(pg_config.postgres_host)
print(pg_config.postgres_port)

# Modify at runtime
pg_config.add_backend_to_all = False
pg_config.backend_id = "custom_backend_id"
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `postgres_host` | str | From env | PostgreSQL server hostname |
| `postgres_port` | int | From env | PostgreSQL server port |
| `postgres_db` | str | From env | Database name |
| `postgres_user` | str | From env | Database username |
| `postgres_password` | str | From env | Database password |
| `backend_id` | str | hostname_ip | Unique identifier for this backend instance |
| `add_backend_to_all` | bool | True | Add backend_id column to all tables |
| `add_backend_to_tables` | bool | True | Append backend_id to table names |
| `raise_on_use_before_init` | bool | True | Raise exception if DB used before initialization |
| `do_publish_db` | bool | False | Enable database publishing features |
| `use_local_db_only` | bool | True | Use only local database connections |

## Quick Start

### 1. Initialize Database Connection

```python
from ry_pg_utils.connect import init_database, ManagedSession
from ry_pg_utils.postgres_info import PostgresInfo

# Create database connection info
db_info = PostgresInfo(
    db_name="myapp_db",
    host="localhost",
    port=5432,
    user="postgres",
    password="secret"
)

# Initialize the database connection
init_database(db_info, db_name="myapp")
```

### 2. Use Dynamic Tables with Protocol Buffers

```python
from ry_pg_utils.dynamic_table import DynamicTableDb
from your_app.proto import YourMessagePb

# Create a message
message = YourMessagePb()
message.field1 = "value1"
message.field2 = 42

# Log message to database (table created automatically)
DynamicTableDb.log_data_to_db(
    msg=message,
    db_name="myapp",
    channel="my_channel"
)

# Check if data exists
exists = DynamicTableDb.is_in_db(
    msg=message,
    db_name="myapp",
    channel="my_channel",
    attr="field1",
    value="value1"
)
```

### 3. Manual Session Management

```python
from ry_pg_utils.connect import ManagedSession

# Use context manager for automatic session cleanup
with ManagedSession(db="myapp") as session:
    result = session.execute("SELECT * FROM my_table")
    for row in result:
        print(row)
```

## Core Components

### `connect.py` - Connection Management

The connection module provides thread-safe database connection and session management:

```python
from ry_pg_utils.connect import (
    init_database,      # Initialize database connection
    init_engine,        # Initialize SQLAlchemy engine
    ManagedSession,     # Context manager for sessions
    get_backend_id,     # Get current backend ID
    set_backend_id,     # Set backend ID for thread
    close_engine,       # Close database connection
    clear_db,           # Clear all connections
)
```

**Key Features:**
- Thread-local backend ID tracking
- Connection pooling with configurable parameters
- Automatic connection recovery on failure
- Session scoping for thread safety

### `dynamic_table.py` - Dynamic Table Creation

Automatically create and manage database tables from Protocol Buffer definitions:

```python
from ry_pg_utils.dynamic_table import DynamicTableDb

# Create instance
db = DynamicTableDb(db_name="myapp")

# Add message to database
db.add_message(
    channel_name="events",
    message_pb=my_protobuf_message,
    log_print_failure=True,
    verbose=True
)

# Check existence
exists = db.inst_is_in_db(
    message_pb=my_protobuf_message,
    channel_name="events",
    attr="event_id",
    value=12345
)
```

**Supported Protocol Buffer Types:**
- `int32`, `int64`, `uint32`, `uint64` → PostgreSQL `Integer`
- `float`, `double` → PostgreSQL `Float`
- `bool` → PostgreSQL `Boolean`
- `string` → PostgreSQL `String`
- `bytes` → PostgreSQL `LargeBinary`
- `Timestamp` (message) → PostgreSQL `DateTime`

### `postgres_info.py` - Connection Information

Data class for PostgreSQL connection parameters:

```python
from ry_pg_utils.postgres_info import PostgresInfo

db_info = PostgresInfo(
    db_name="mydb",
    host="localhost",
    port=5432,
    user="postgres",
    password="secret"
)

# Get connection URI
uri = db_info.get_uri()  # postgresql://postgres:secret@localhost:5432/mydb
```

### `parse_args.py` - Argument Parsing

Add PostgreSQL arguments to your argument parser:

```python
import argparse
from ry_pg_utils.parse_args import add_postrgres_db_args

parser = argparse.ArgumentParser()
add_postrgres_db_args(parser)

args = parser.parse_args()
# Access: args.postgres_host, args.postgres_port, etc.
```

### `updater.py` - Database Configuration Updater

Dynamically update database connections based on configuration messages:

```python
from ry_pg_utils.updater import PostgresDbUpdater

updater = PostgresDbUpdater(
    redis_info=redis_info,
    verbose=verbose,
    backend_id="my_backend"
)

# Start listening for configuration updates
updater.run()
```

## Advanced Usage

### Multi-Backend Support

When `add_backend_to_all` is enabled, all tables automatically get a `backend_id` column:

```python
from ry_pg_utils.connect import set_backend_id, ManagedSession

# Set backend ID for current thread
set_backend_id("backend_1")

# All subsequent operations will include this backend_id
with ManagedSession(db="myapp") as session:
    # Queries automatically filter by backend_id
    result = session.execute("SELECT * FROM my_table")
```

### Custom Table Names

When `add_backend_to_tables` is enabled, table names are automatically suffixed:

```python
from ry_pg_utils.connect import get_table_name

# Returns "events_my_backend" if add_backend_to_tables=True
table_name = get_table_name("events", backend_id="my_backend")
```

### ORM Base Class

Use the pre-configured base class for SQLAlchemy models:

```python
from ry_pg_utils.connect import Base
from sqlalchemy import Column, Integer, String

class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    email = Column(String(200))

# If add_backend_to_all=True, backend_id column is automatically added
```

## Error Handling

The library includes robust error handling:

```python
from ry_pg_utils.connect import ManagedSession

with ManagedSession(db="myapp") as session:
    if session is None:
        # Connection failed, handle gracefully
        print("Failed to establish database connection")
        return

    try:
        session.execute("SELECT * FROM my_table")
    except Exception as e:
        # Session will automatically rollback
        print(f"Query failed: {e}")
```

## Type Safety

The library is fully typed and includes a `py.typed` marker for mypy support:

```bash
# Run type checking
mypy your_app.py
```

## Development

### Setup Development Environment

```bash
# Clone the repository
git clone https://github.com/yourusername/ry-pg-utils.git
cd ry-pg-utils

# Create virtual environment
python -m venv venv-dev
source venv-dev/bin/activate  # On Windows: venv-dev\Scripts\activate

# Install dependencies
pip install -r packages/requirements-dev.txt
```

### Running Tests

```bash
# Activate virtual environment
source venv-dev/bin/activate

# Run tests
python -m pytest test/
```

### Code Quality

The project uses several tools for code quality:

```bash
# Format code
black ry_pg_utils/

# Type checking
mypy ry_pg_utils/

# Linting
pylint ry_pg_utils/

# Import sorting
isort ry_pg_utils/
```

## Examples

### Complete Application Example

```python
import argparse
from ry_pg_utils.parse_args import add_postrgres_db_args
from ry_pg_utils.connect import init_database, ManagedSession
from ry_pg_utils.postgres_info import PostgresInfo
from ry_pg_utils.dynamic_table import DynamicTableDb

def parse_args():
    parser = argparse.ArgumentParser(description="My Database App")
    add_postrgres_db_args(parser)
    return parser.parse_args()

def main():
    args = parse_args()

    # Initialize database
    db_info = PostgresInfo(
        db_name=args.postgres_db,
        host=args.postgres_host,
        port=args.postgres_port,
        user=args.postgres_user,
        password=args.postgres_password
    )

    init_database(db_info, db_name="myapp")

    # Use the database
    with ManagedSession(db="myapp") as session:
        if session:
            result = session.execute("SELECT version()")
            print(f"PostgreSQL version: {result.fetchone()[0]}")

if __name__ == "__main__":
    main()
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Author

Ross Yeager - ryeager12@email.com

## Changelog

### Version 1.0.0
- Initial release
- Database connection management
- Dynamic table creation
- Multi-backend support
- Configuration system
- Protocol Buffer integration

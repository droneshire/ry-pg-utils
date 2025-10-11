# ry-pg-utils

A Python utility library for PostgreSQL database operations with dynamic table creation, connection management, Protocol Buffer integration, and PostgreSQL LISTEN/NOTIFY support.

## Overview

`ry-pg-utils` provides a robust framework for working with PostgreSQL databases in Python applications. It includes utilities for:

- Database connection management with connection pooling
- Dynamic table creation from Protocol Buffer message definitions
- Thread-safe session management
- Multi-backend support with automatic backend ID tracking
- PostgreSQL LISTEN/NOTIFY triggers and notifications
- Database updater for dynamic configuration via Redis
- Argument parsing for PostgreSQL connection parameters

## Features

- **Connection Management**: Thread-safe PostgreSQL connection pooling with automatic retry logic and health checks
- **Dynamic Tables**: Automatically create and manage database tables from Protocol Buffer message schemas
- **Multi-Backend Support**: Track data across multiple backend instances with automatic ID tagging
- **Session Management**: Context managers for safe database session handling
- **Notification System**: Built-in PostgreSQL LISTEN/NOTIFY support with triggers and callbacks
- **Configuration System**: Flexible configuration via environment variables and runtime settings
- **Redis Integration**: Optional Redis-based database configuration updates
- **Type Safety**: Full type hints and mypy support

## Installation

```bash
pip install ry-pg-utils
```

### Dependencies

- Python 3.12+
- PostgreSQL database
- SQLAlchemy 2.0+
- Protocol Buffer support (protobuf)
- psycopg2-binary
- tenacity (for retry logic)
- python-dotenv (for environment variables)
- ryutils (logging utilities)
- ry_redis_bus (optional, for Redis integration)

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
| `backend_id` | str | POSTGRES_USER or hostname_ip | Unique identifier for this backend instance |
| `add_backend_to_all` | bool | True | Add backend_id column to all tables |
| `add_backend_to_tables` | bool | True | Append backend_id to table names |
| `raise_on_use_before_init` | bool | True | Raise exception if DB used before initialization |
| `do_publish_db` | bool | False | Enable database publishing features (for Redis integration) |
| `use_local_db_only` | bool | True | Use only local database connections |

## Quick Start

### 1. Initialize Database Connection

```python
from ry_pg_utils.connect import init_database, ManagedSession

# Initialize the database connection
init_database(
    db_name="myapp_db",
    db_host="localhost",
    db_port=5432,
    db_user="postgres",
    db_password="secret"
)
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
from sqlalchemy import text

# Use context manager for automatic session cleanup
with ManagedSession(db="myapp_db") as session:
    if session:
        result = session.execute(text("SELECT * FROM my_table"))
        for row in result:
            print(row)
```

## Core Components

### `connect.py` - Connection Management

The connection module provides thread-safe database connection and session management:

```python
from ry_pg_utils.connect import (
    init_database,           # Initialize database connection
    init_engine,             # Initialize SQLAlchemy engine
    ManagedSession,          # Context manager for sessions
    get_backend_id,          # Get current backend ID
    set_backend_id,          # Set backend ID for thread
    get_engine,              # Get engine for database
    close_engine,            # Close database connection
    clear_db,                # Clear all connections
    get_table_name,          # Get table name with backend suffix
    is_database_initialized, # Check if database is initialized
    Base,                    # SQLAlchemy declarative base
)
```

**Key Features:**
- Thread-local backend ID tracking
- Connection pooling with configurable parameters (pool_size, max_overflow, pool_recycle)
- Automatic connection recovery with retry logic (using tenacity)
- Session scoping for thread safety
- Automatic backend_id injection before flush operations
- Pre-ping health checks to validate connections
- Support for auto-importing model modules

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

Class for PostgreSQL connection parameters:

```python
from ry_pg_utils.postgres_info import PostgresInfo

db_info = PostgresInfo(
    db_name="mydb",
    host="localhost",
    port=5432,
    user="postgres",
    password="secret"
)

# Check if valid
if not db_info.is_null():
    print(db_info)  # Prints info with password masked

# Create null instance
null_info = PostgresInfo.null()
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

Dynamically update database connections based on configuration messages via Redis:

```python
from ry_pg_utils.updater import DbUpdater
from ry_redis_bus.helpers import RedisInfo
from ryutils.verbose import Verbose

updater = DbUpdater(
    redis_info=redis_info,
    args=args,  # argparse.Namespace with postgres config
    backend_id="my_backend",
    verbose=Verbose(True)
)

# Initialize and start listening for configuration updates
updater.init()

# Run the update loop
updater.run()  # Blocks, continuously checking for updates
```

### `notify_trigger.py` - PostgreSQL LISTEN/NOTIFY Support

Create database triggers that send notifications on table changes:

```python
from ry_pg_utils.notify_trigger import (
    create_notify_trigger,
    drop_notify_trigger,
    subscribe_to_notifications,
    NotificationListener,
)
from ry_pg_utils.connect import get_engine

engine = get_engine("myapp_db")

# Create a trigger that notifies on INSERT/UPDATE/DELETE
create_notify_trigger(
    engine=engine,
    table_name="users",
    channel_name="user_changes",
    events=["INSERT", "UPDATE", "DELETE"],
    columns=["id", "username", "email"]  # Optional: only include specific columns
)

# Subscribe to notifications
def handle_notification(notification):
    print(f"Table: {notification['table']}")
    print(f"Action: {notification['action']}")
    print(f"Data: {notification['data']}")

with subscribe_to_notifications(
    engine=engine,
    channel_name="user_changes",
    callback=handle_notification,
    timeout=60.0
) as notifications:
    # Notifications are handled in background thread
    time.sleep(10)

# Or use the NotificationListener class for long-running listeners
listener = NotificationListener(db_name="myapp_db")
listener.create_listener(
    table_name="users",
    channel_name="user_changes",
    events=["INSERT", "UPDATE"]
)
listener.add_callback("user_changes", handle_notification)
listener.start()

# ... your application code ...

listener.stop()
```

### `ipc/channels.py` - Redis Communication Channels

Pre-defined Redis channels for database configuration updates (requires ry_redis_bus):

```python
from ry_pg_utils.ipc.channels import (
    DATABASE_CHANNEL,           # DatabaseConfigPb messages
    DATABASE_CONFIG_CHANNEL,    # DatabaseSettingsPb messages
    DATABASE_NOTIFY_CHANNEL,    # DatabaseNotificationPb messages
)

# These channels are used by DbUpdater for dynamic database configuration
```

## Advanced Usage

### Multi-Backend Support

When `add_backend_to_all` is enabled (default: True), all tables automatically get a `backend_id` column:

```python
from ry_pg_utils.connect import set_backend_id, ManagedSession
from sqlalchemy import text

# Set backend ID for current thread
set_backend_id("backend_1")

# All subsequent operations will include this backend_id
# ManagedSession automatically sets the backend_id if provided
with ManagedSession(db="myapp_db", backend_id="backend_1") as session:
    if session:
        # New/dirty records automatically get backend_id injected before flush
        result = session.execute(text("SELECT * FROM my_table"))
```

### Custom Table Names

When `add_backend_to_tables` is enabled (default: True), table names are automatically suffixed:

```python
from ry_pg_utils.connect import get_table_name
from ry_pg_utils.config import pg_config

# Returns "events_my_backend" if add_backend_to_tables=True
table_name = get_table_name("events", backend_id="my_backend", verbose=True)

# Configure globally
pg_config.add_backend_to_tables = False  # Disable backend suffix
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

# If add_backend_to_all=True (default), backend_id column is automatically added
# The backend_id is a String(256) column, nullable=False
```

### Auto-Importing Models

Use the `models_module` parameter to automatically import all models before table creation:

```python
from ry_pg_utils.connect import init_database

# This will walk through 'myapp.models' and import all submodules
# ensuring all model classes are registered with Base.metadata
init_database(
    db_name="myapp_db",
    db_host="localhost",
    db_port=5432,
    db_user="postgres",
    db_password="secret",
    models_module="myapp.models"  # Dot-separated module path
)
```

## Error Handling

The library includes robust error handling with automatic retries:

```python
from ry_pg_utils.connect import ManagedSession
from sqlalchemy import text

with ManagedSession(db="myapp_db") as session:
    if session is None:
        # Connection failed after retries, handle gracefully
        print("Failed to establish database connection")
        return

    try:
        session.execute(text("SELECT * FROM my_table"))
    except Exception as e:
        # Session will automatically rollback on exception
        print(f"Query failed: {e}")

# Retries are built-in:
# - Session operations retry 3 times on OperationalError
# - Exponential backoff: min 4s, max 10s
# - Connection health checks via pool_pre_ping
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

Tests require a running PostgreSQL instance. Configure test database connection via environment variables or `.env` file.

```bash
# Activate virtual environment
source venv-dev/bin/activate

# Run all tests
make test

# Run specific test module
make test TESTMODULE=connect_test
```

### Code Quality

The project uses several tools for code quality:

```bash
# Format code
make format

# Run linting
make lint_full

# Type checking is included in lint_full (uses mypy)
```

## Examples

### Complete Application Example

```python
import argparse
from ry_pg_utils.parse_args import add_postrgres_db_args
from ry_pg_utils.connect import init_database, ManagedSession
from ry_pg_utils.dynamic_table import DynamicTableDb
from sqlalchemy import text

def parse_args():
    parser = argparse.ArgumentParser(description="My Database App")
    add_postrgres_db_args(parser)
    return parser.parse_args()

def main():
    args = parse_args()

    # Initialize database
    init_database(
        db_name=args.postgres_db,
        db_host=args.postgres_host,
        db_port=args.postgres_port,
        db_user=args.postgres_user,
        db_password=args.postgres_password
    )

    # Use the database
    with ManagedSession(db=args.postgres_db) as session:
        if session:
            result = session.execute(text("SELECT version()"))
            print(f"PostgreSQL version: {result.fetchone()[0]}")

if __name__ == "__main__":
    main()
```

### Real-time Notification Example

```python
from ry_pg_utils.connect import init_database, get_engine, Base
from ry_pg_utils.notify_trigger import create_notify_trigger, NotificationListener
from sqlalchemy import Column, Integer, String
import time

# Define a model
class Product(Base):
    __tablename__ = 'products'

    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    price = Column(Integer)

# Initialize database
init_database(
    db_name="inventory_db",
    db_host="localhost",
    db_port=5432,
    db_user="postgres",
    db_password="secret"
)

# Create notification trigger
engine = get_engine("inventory_db")
create_notify_trigger(
    engine=engine,
    table_name="products",
    channel_name="product_updates",
    events=["INSERT", "UPDATE", "DELETE"],
    columns=["id", "name", "price"]
)

# Set up listener
listener = NotificationListener(db_name="inventory_db")

def on_product_change(notification):
    action = notification['action']
    data = notification['data']
    print(f"Product {action}: {data}")

listener.add_callback("product_updates", on_product_change)
listener.start()

# Your application runs...
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    listener.stop()
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

### Version 1.0.2 (Current)
- PostgreSQL LISTEN/NOTIFY support with triggers and notifications
- NotificationListener class for background notification handling
- Automatic connection health checks with pool_pre_ping
- Auto-importing models from specified module paths
- Enhanced retry logic with tenacity
- Improved error handling and connection recovery

### Version 1.0.0
- Initial release
- Database connection management with pooling
- Dynamic table creation from Protocol Buffers
- Multi-backend support with automatic ID tagging
- Configuration system with environment variables
- Protocol Buffer integration
- Redis-based database configuration updates

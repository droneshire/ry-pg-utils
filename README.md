# Redis IPC

[![Python application](https://github.com/droneshire/ry-redis-bus/actions/workflows/python-app.yml/badge.svg)](https://github.com/droneshire/ry-redis-bus/actions/workflows/python-app.yml)
[![Upload Python Package](https://github.com/droneshire/ry-redis-bus/actions/workflows/python-publish.yml/badge.svg)](https://github.com/droneshire/ry-redis-bus/actions/workflows/python-publish.yml)

A Python library for Redis-based Inter-Process Communication (IPC) with built-in logging capabilities.

## Overview

Redis IPC provides a robust framework for implementing inter-process communication using Redis pub/sub mechanisms. It includes utilities for:

- Redis client management (both sync and async)
- Channel-based message publishing and subscription
- Automatic message logging to PostgreSQL databases
- Verbose logging and debugging support
- Protocol Buffer message handling

## Features

- **Redis Client Management**: Base classes for both synchronous and asynchronous Redis operations
- **Channel Management**: Easy-to-use channel-based messaging system
- **Message Logging**: Automatic logging of all IPC messages to PostgreSQL
- **Protocol Buffer Support**: Built-in support for Protocol Buffer message types
- **Verbose Logging**: Comprehensive logging and debugging capabilities
- **Database Integration**: Seamless integration with PostgreSQL for message persistence

## Installation

```bash
pip install ryutils
```

## Quick Start

Here's a complete example of how to use the Redis IPC Logger:

```python
import argparse

from ryutils.verbose import Verbose

from config import constants
from database.parse_args import add_postgres_db_args
from ipc.ipc_logger import IpcLogger
from ipc.redis_args import add_redis_args


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Redis IPC Logger")

    add_postgres_db_args(parser)
    add_redis_args(parser)

    Verbose.add_arguments(parser, verbose_types=constants.VERBOSE_TYPES)

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    verbose = Verbose(args=args, print_args=True, verbose_types=constants.VERBOSE_TYPES)

    ipc_logger = IpcLogger(verbose=verbose, args=args)
    ipc_logger.subscribe_all()
    ipc_logger.run()


if __name__ == "__main__":
    main()
```

## Usage

### 1. Setting Up Arguments

The library provides argument parsers for both Redis and PostgreSQL configuration:

```python
from ipc.redis_args import add_redis_args
from database.parse_args import add_postgres_db_args

parser = argparse.ArgumentParser()
add_redis_args(parser)  # Adds Redis host, port, db, user, password, db_name
add_postgres_db_args(parser)  # Adds PostgreSQL connection arguments
```

### 2. Creating an IPC Logger

```python
from ipc.ipc_logger import IpcLogger

ipc_logger = IpcLogger(verbose=verbose, args=args)
```

### 3. Subscribing to Messages

```python
# Subscribe to all channels
ipc_logger.subscribe_all()

# Start the logger
ipc_logger.run()
```

## Architecture

The library is built around several key components:

- **`RedisClientBase`**: Base class for Redis operations
- **`IpcLogger`**: Main logging class that subscribes to Redis channels
- **`RedisReceiver`**: Handles Redis message reception
- **`Channels`**: Manages channel-based messaging
- **`Helpers`**: Utility functions for common operations

## Configuration

### Redis Configuration

- `redis_host`: Redis server hostname
- `redis_port`: Redis server port
- `redis_db`: Redis database number
- `redis_user`: Redis username (if authentication is enabled)
- `redis_password`: Redis password
- `redis_db_name`: Redis database name

### PostgreSQL Configuration

The library automatically logs all IPC messages to a PostgreSQL database using the `LogIpcMessage` table.

## Message Format

Messages are automatically converted to Protocol Buffer format and include:

- Message content
- Channel name
- Timestamp (UTC)

## Development

### Requirements

- Python 3.12+
- Redis server
- PostgreSQL database
- Protocol Buffer support

### Running Tests

```bash
# Activate virtual environment first
source venv/bin/activate

# Run tests
python -m pytest test/
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

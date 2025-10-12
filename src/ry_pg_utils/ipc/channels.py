from ry_redis_bus.channels import Channel

from ry_pg_utils.pb_types.database_pb2 import (  # pylint: disable=no-name-in-module
    DatabaseNotificationPb,
    PostgresMessagePb,
)

# Channels
DATABASE_CHANNEL = Channel("DATABASE_CHANNEL", PostgresMessagePb)
DATABASE_CONFIG_CHANNEL = Channel("DATABASE_CONFIG_CHANNEL", PostgresMessagePb)
DATABASE_NOTIFY_CHANNEL = Channel("DATABASE_NOTIFY_CHANNEL", DatabaseNotificationPb)

import argparse
import json
import time
import typing as T
from dataclasses import dataclass, field

from ry_redis_bus.helpers import RedisInfo, message_handler
from ry_redis_bus.redis_client_base import RedisClientBase
from ryutils import log
from ryutils.verbose import Verbose

from ry_pg_utils.connect import close_engine, init_database, is_database_initialized
from ry_pg_utils.ipc import channels
from ry_pg_utils.notify_trigger import NotificationListener
from ry_pg_utils.pb_types.database_pb2 import (  # pylint: disable=no-name-in-module
    DatabaseNotificationPb,
    PostgresMessagePb,
    PostgresPb,
)
from ry_pg_utils.postgres_info import PostgresInfo

DB_RETRY_TIME = 5.0


def get_database_settings(
    raw_db_config_pb: PostgresMessagePb | None, verbose: bool = False
) -> T.Optional[PostgresInfo]:
    if raw_db_config_pb is None:
        return None

    db_config_pb: PostgresPb = raw_db_config_pb.postgres

    if verbose:
        log.print_normal(f"Received database settings:\n{db_config_pb}")

    database_settings_msg = PostgresInfo(
        db_name=db_config_pb.database,
        user=db_config_pb.user,
        password=db_config_pb.password,
        host=db_config_pb.host,
        port=db_config_pb.port,
    )

    return database_settings_msg


@dataclass
class DbUpdater(RedisClientBase):
    # Init parameters
    redis_info: RedisInfo
    args: argparse.Namespace
    verbose: Verbose
    logging_error_db_callback: T.Optional[T.Callable[[str, str], None]] = None
    models_module: T.Optional[str] = None
    subscribe_details: T.Optional[T.Dict[str, T.List[str]]] = None

    # Computed fields (not in __init__)
    do_publish_db: bool = field(init=False)
    postgres_info: PostgresInfo = field(init=False)
    last_db_init_retry_time: float = field(init=False, default=0.0)
    use_local_db_only: bool = field(init=False)
    database_settings_msg: T.Optional[PostgresInfo] = field(init=False, default=None)
    notify_trigger: NotificationListener = field(init=False)

    def __post_init__(self) -> None:
        """Initialize computed fields after dataclass initialization."""
        super().__init__(
            redis_info=self.redis_info,
            verbose=self.verbose,
        )

        self.do_publish_db = self.args.do_publish_db if self.args else True

        self.postgres_info = PostgresInfo(
            db_name=self.args.postgres_db,
            user=self.args.postgres_user,
            password=self.args.postgres_password,
            host=self.args.postgres_host,
            port=self.args.postgres_port,
        )

        self.last_db_init_retry_time = 0.0
        self.use_local_db_only = self.args.use_local_db_only if self.args else True
        self.database_settings_msg = None
        self.notify_trigger = NotificationListener(self.postgres_info.db_name)

    @message_handler
    def handle_database_config_message(self, message_pb: PostgresMessagePb) -> None:
        database_settings = get_database_settings(message_pb)

        if database_settings is not None:
            self.database_settings_msg = database_settings

    def init(self) -> None:
        super().start()

        if self.use_local_db_only:
            init_database(
                db_host=self.postgres_info.host,
                db_port=self.postgres_info.port,
                db_name=self.postgres_info.db_name,
                db_user=self.postgres_info.user,
                db_password=self.postgres_info.password,
                models_module=self.models_module,
            )
        else:
            self.postgres_info = PostgresInfo.null()
            log.print_fail(f"DbUpdater initialized with null database info: {self.postgres_info}")

            self.subscribe(channels.DATABASE_CONFIG_CHANNEL, self.handle_database_config_message)

        if self.subscribe_details:
            for table, columns in self.subscribe_details.items():
                # pylint: disable=no-member
                channel = self.get_channel_name(table)  # type: ignore[attr-defined]
                self.notify_trigger.create_listener(
                    table_name=table, channel_name=channel, columns=columns if columns else None
                )
                # pylint: disable=no-member
                self.notify_trigger.add_callback(
                    channel, self._handle_database_notification  # type: ignore[attr-defined]
                )

            self.notify_trigger.start()

    def _handle_database_notification(self, notification: T.Dict[str, T.Any]) -> None:
        log.print_normal(f"Received notification: {notification}")
        notification_pb = DatabaseNotificationPb()
        notification_pb.utime.GetCurrentTime()
        notification_pb.table_name = notification["table"]
        notification_pb.channel_name = self.get_channel_name(notification["table"])
        notification_pb.action = notification["action"]
        notification_pb.payload = json.dumps(notification["data"])
        self.publish(channels.DATABASE_NOTIFY_CHANNEL, notification_pb.SerializeToString())

    @staticmethod
    def get_channel_name(table_name: str) -> str:
        return f"{table_name}_notify"

    def update_db(self, postgres_info: PostgresInfo) -> None:
        self.postgres_info = postgres_info

    def get_info(self) -> T.Optional[PostgresInfo]:
        return self.postgres_info

    def step(self, force: bool = False) -> None:
        now = time.time()

        super().step()

        if not force and now - self.last_db_init_retry_time < DB_RETRY_TIME:
            return

        self.last_db_init_retry_time = now

        self.maybe_update_database()

    def maybe_update_database(self) -> None:
        new_postgres_info: T.Optional[PostgresInfo] = (
            self.postgres_info if self.database_settings_msg is None else self.database_settings_msg
        )

        self.database_settings_msg = None

        if new_postgres_info is None:
            return

        if new_postgres_info.is_null():
            log.print_fail(f"Database info is null: {new_postgres_info}")
            return

        if new_postgres_info == self.postgres_info and is_database_initialized(
            new_postgres_info.db_name
        ):
            return

        log.print_bold(f"Updating database to {new_postgres_info}")

        close_engine(self.postgres_info.db_name)
        init_database(
            db_host=new_postgres_info.host,
            db_port=new_postgres_info.port,
            db_name=new_postgres_info.db_name,
            db_user=new_postgres_info.user,
            db_password=new_postgres_info.password,
            models_module=self.models_module,
        )

        self.postgres_info = new_postgres_info

        db_message_pb = PostgresMessagePb()
        postgres_db_pb = PostgresPb()
        postgres_db_pb.database = self.postgres_info.db_name
        postgres_db_pb.user = self.postgres_info.user
        postgres_db_pb.password = self.postgres_info.password
        postgres_db_pb.host = self.postgres_info.host
        postgres_db_pb.port = self.postgres_info.port
        db_message_pb.postgres.CopyFrom(postgres_db_pb)
        db_message_pb.utime.GetCurrentTime()
        if self.do_publish_db:
            self.publish(channels.DATABASE_CHANNEL, db_message_pb.SerializeToString())

        def log_print_callback(message: str) -> None:
            if self.logging_error_db_callback:
                self.logging_error_db_callback(message, self.postgres_info.db_name)

        log.update_callback(log_print_callback)

    def run(self) -> None:
        while True:
            self.step()
            time.sleep(1)

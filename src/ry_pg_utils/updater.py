import argparse
import time
import typing as T
from dataclasses import dataclass

from ry_redis_bus.helpers import RedisInfo, message_handler
from ry_redis_bus.redis_client_base import RedisClientBase
from ryutils import log
from ryutils.verbose import Verbose

from ry_pg_utils.connect import close_engine, init_database, is_database_initialized
from ry_pg_utils.ipc import channels
from ry_pg_utils.pb_types.database_pb2 import (  # pylint: disable=no-name-in-module
    DatabaseSettingsPb,
    PostgresMessagePb,
    PostgresPb,
)
from ry_pg_utils.postgres_info import PostgresInfo

DB_RETRY_TIME = 5.0


def get_database_settings(
    message_pb: DatabaseSettingsPb, backend_id: str, verbose: bool = False
) -> T.Optional[PostgresInfo]:
    raw_db_config_pb = message_pb.postgres.get(backend_id, None)

    if raw_db_config_pb is None:
        return None

    if not message_pb.primaryDatabase:
        return None

    db_config_pb: PostgresPb = raw_db_config_pb

    if verbose:
        log.print_normal(f"Received database settings {backend_id}:\n{db_config_pb}")

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
    do_publish_db: bool
    postgres_info: PostgresInfo
    backend_id: str
    logging_error_db_callback: T.Callable[[str, str], None] | None

    def __init__(
        self,
        redis_info: RedisInfo,
        args: argparse.Namespace,
        backend_id: str,
        verbose: Verbose,
        logging_error_db_callback: T.Callable[[str, str], None] | None = None,
    ):
        super().__init__(
            redis_info=redis_info,
            verbose=verbose,
        )

        self.do_publish_db = args.do_publish_db if args else True

        self.postgres_info = PostgresInfo(
            db_name=args.postgres_db,
            user=args.postgres_user,
            password=args.postgres_password,
            host=args.postgres_host,
            port=args.postgres_port,
        )

        self.last_db_init_retry_time = 0.0
        self.use_local_db_only = args.use_local_db_only if args else True
        self.database_settings_msg: T.Optional[PostgresInfo] = None
        self.backend_id = backend_id
        self.logging_error_db_callback = logging_error_db_callback

    @message_handler
    def handle_database_config_message(self, message_pb: DatabaseSettingsPb) -> None:
        database_settings = get_database_settings(message_pb, backend_id=self.backend_id)

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
            )
        else:
            self.postgres_info = PostgresInfo.null()
            log.print_fail(f"DbUpdater initialized with null database info: {self.postgres_info}")

            self.subscribe(channels.DATABASE_CONFIG_CHANNEL, self.handle_database_config_message)

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

import argparse

from . import config


def add_postgres_db_args(parser: argparse.ArgumentParser) -> None:
    postgres_parser = parser.add_argument_group("postgres-options")
    postgres_parser.add_argument("--postgres-host", default=config.pg_config.postgres_host)
    postgres_parser.add_argument(
        "--postgres-port", type=int, default=config.pg_config.postgres_port
    )
    postgres_parser.add_argument("--postgres-db", default=config.pg_config.postgres_db)
    postgres_parser.add_argument("--postgres-user", default=config.pg_config.postgres_user)
    postgres_parser.add_argument("--postgres-password", default=config.pg_config.postgres_password)
    postgres_parser.add_argument("--do-publish-db", action="store_true", default=False)

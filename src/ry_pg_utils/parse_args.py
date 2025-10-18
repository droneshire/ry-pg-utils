import argparse

from ry_pg_utils.config import get_config


def add_postgres_db_args(parser: argparse.ArgumentParser) -> None:
    postgres_parser = parser.add_argument_group("postgres-options")
    config = get_config()
    postgres_parser.add_argument("--postgres-host", default=config.postgres_host)
    postgres_parser.add_argument("--postgres-port", type=int, default=config.postgres_port)
    postgres_parser.add_argument("--postgres-db", default=config.postgres_db)
    postgres_parser.add_argument("--postgres-user", default=config.postgres_user)
    postgres_parser.add_argument("--postgres-password", default=config.postgres_password)
    postgres_parser.add_argument("--do-publish-db", action="store_true", default=False)
    postgres_parser.add_argument(
        "--use-local-db-only",
        action="store_true",
        default=config.use_local_db_only,
        help="Use only local database connections (no Redis-based config updates)",
    )

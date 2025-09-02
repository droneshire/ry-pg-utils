import argparse

from config import constants


def add_postrgres_db_args(parser: argparse.ArgumentParser) -> None:
    postgres_parser = parser.add_argument_group("postgres-options")
    postgres_parser.add_argument("--postgres-host", default=constants.POSTGRES_HOST)
    postgres_parser.add_argument("--postgres-port", type=int, default=constants.POSTGRES_PORT)
    postgres_parser.add_argument("--postgres-db", default=constants.POSTGRES_DB)
    postgres_parser.add_argument("--postgres-user", default=constants.POSTGRES_USER)
    postgres_parser.add_argument("--postgres-password", default=constants.POSTGRES_PASSWORD)
    postgres_parser.add_argument("--do-publish-db", action="store_true", default=False)

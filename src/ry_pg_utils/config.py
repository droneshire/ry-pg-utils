import os
import socket
from dataclasses import dataclass

import dotenv


@dataclass
class Config:
    postgres_host: str | None
    postgres_port: int | None
    postgres_db: str | None
    postgres_user: str | None
    postgres_password: str | None
    do_publish_db: bool
    use_local_db_only: bool
    backend_id: str
    add_backend_to_all: bool
    add_backend_to_tables: bool
    raise_on_use_before_init: bool


dotenv.load_dotenv()

# Parse POSTGRES_PORT with proper None handling for mypy
_postgres_port_str = os.getenv("POSTGRES_PORT")
_postgres_port = int(_postgres_port_str) if _postgres_port_str is not None else None

pg_config = Config(
    postgres_host=os.getenv("POSTGRES_HOST"),
    postgres_port=_postgres_port,
    postgres_db=os.getenv("POSTGRES_DB"),
    postgres_user=os.getenv("POSTGRES_USER"),
    postgres_password=os.getenv("POSTGRES_PASSWORD"),
    do_publish_db=False,
    use_local_db_only=True,
    backend_id=(
        os.getenv("POSTGRES_USER")
        or f"{socket.gethostname()}_{socket.gethostbyname(socket.gethostname())}"
    ),
    add_backend_to_all=True,
    add_backend_to_tables=True,
    raise_on_use_before_init=True,
)

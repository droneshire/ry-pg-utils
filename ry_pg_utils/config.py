from dataclasses import dataclass
import os
import socket

import dotenv


@dataclass
class Config:
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    do_publish_db: bool
    use_local_db_only: bool
    backend_id: str
    add_backend_to_all: bool
    add_backend_to_tables: bool
    raise_on_use_before_init: bool


dotenv.load_dotenv()
pg_config = Config(
    postgres_host=os.getenv("POSTGRES_HOST"),
    postgres_port=os.getenv("POSTGRES_PORT"),
    postgres_db=os.getenv("POSTGRES_DB"),
    postgres_user=os.getenv("POSTGRES_USER"),
    postgres_password=os.getenv("POSTGRES_PASSWORD"),
    do_publish_db=False,
    use_local_db_only=True,
    backend_id=f"{socket.gethostname()}_{socket.gethostbyname(socket.gethostname())}",
    add_backend_to_all=True,
    add_backend_to_tables=True,
    raise_on_use_before_init=True,
)

import contextlib
import threading
import typing as T

from . import config
from ryutils import log
from sqlalchemy import Column, String, create_engine, event
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import (declarative_base, declared_attr, scoped_session,
                            sessionmaker)
from sqlalchemy.orm.scoping import ScopedSession
from sqlalchemy_utils import database_exists

_thread_local = threading.local()
BACKEND_ID_VARIABLE = "backend_id"

ENGINE: T.Dict[str, Engine] = {}
THREAD_SAFE_SESSION_FACTORY: T.Dict[str, ScopedSession] = {}

# Base class with optional backend_id field
if config.pg_config.add_backend_to_all:
    # Add any common fields here
    class CommonBaseModel:
        @declared_attr
        def backend_id(cls: T.Any) -> Column:  # pylint: disable=no-self-argument
            return Column(String(256), nullable=False)

    Base = declarative_base(name="Base", cls=CommonBaseModel)
else:
    Base = declarative_base(name="Base")


def get_table_name(base_name: str, verbose: bool = False, backend_id: str = config.pg_config.backend_id) -> str:
    if verbose:
        print(f"{base_name}_{backend_id}" if config.pg_config.add_backend_to_tables else f"{base_name}")
    return f"{base_name}_{backend_id}" if config.pg_config.add_backend_to_tables else base_name


def init_engine(uri: str, db: str, **kwargs: T.Any) -> Engine:
    global ENGINE  # pylint: disable=global-variable-not-assigned
    if db not in ENGINE:
        defaults = {
            "pool_recycle": 3600,
            "pool_pre_ping": True,
            "pool_size": 5,
            "max_overflow": 10,
        }
        for key, val in defaults.items():
            kwargs.setdefault(key, val)
        ENGINE[db] = create_engine(uri, **kwargs)
    return ENGINE[db]


def clear_db() -> None:
    global ENGINE  # pylint: disable=global-statement
    global THREAD_SAFE_SESSION_FACTORY  # pylint: disable=global-statement
    ENGINE = {}
    THREAD_SAFE_SESSION_FACTORY = {}


def close_engine(db: str) -> None:
    global ENGINE  # pylint: disable=global-statement, global-variable-not-assigned
    global THREAD_SAFE_SESSION_FACTORY  # pylint: disable=global-statement, global-variable-not-assigned
    if db in ENGINE:
        ENGINE[db].dispose()
        del ENGINE[db]
    THREAD_SAFE_SESSION_FACTORY.pop(db, None)


def _init_session_factory(db: str) -> ScopedSession:
    """Initialize the THREAD_SAFE_SESSION_FACTORY."""
    global ENGINE, THREAD_SAFE_SESSION_FACTORY  # pylint: disable=global-variable-not-assigned
    if db not in ENGINE:
        raise ValueError("Call init_engine before initializing session factory for {db}!")
    if db not in THREAD_SAFE_SESSION_FACTORY:
        factory = sessionmaker(bind=ENGINE[db])
        THREAD_SAFE_SESSION_FACTORY[db] = scoped_session(factory)
    return THREAD_SAFE_SESSION_FACTORY[db]


def set_backend_id(backend_id: str) -> None:
    setattr(_thread_local, BACKEND_ID_VARIABLE, backend_id)


def get_backend_id() -> T.Optional[str]:
    return getattr(_thread_local, BACKEND_ID_VARIABLE, None)


@event.listens_for(scoped_session, "before_flush")
def receive_before_flush(session: ScopedSession, _flush_context: T.Any, _instances: T.Any) -> None:
    backend_id = get_backend_id()
    if not backend_id:
        return
    for inst in session.dirty | session.new:
        if getattr(inst, "backend_id", None) is None:
            inst.backend_id = backend_id


def is_session_factory_initialized() -> bool:
    return bool(THREAD_SAFE_SESSION_FACTORY)


@contextlib.contextmanager
def ManagedSession(  # pylint: disable=invalid-name
    db: T.Optional[str] = None, backend_id: T.Optional[str] = config.pg_config.backend_id
) -> T.Iterator[T.Optional[ScopedSession]]:
    """Get a session object whose lifecycle, commits and flush are managed for you.
    The session will automatically retry operations on connection errors.

    Expected to be used as follows:
    ```
    # multiple db_operations are done within one session.
    with ManagedSession() as session:
        # db_operations is expected not to worry about session handling.
        db_operations.select(session, **kwargs)
        # after the with statement, the session commits to the database.
        db_operations.insert(session, **kwargs)
    ```
    """
    global THREAD_SAFE_SESSION_FACTORY  # pylint: disable=global-variable-not-assigned
    if db is None:
        db = next(iter(THREAD_SAFE_SESSION_FACTORY), None)

    if not db or db not in THREAD_SAFE_SESSION_FACTORY:
        if config.pg_config.raise_on_use_before_init:
            raise ValueError(f"Session factory for {db} not initialized.")
        log.print_fail(f"Session factory for {db} not initialized.")
        yield None
        return

    session = THREAD_SAFE_SESSION_FACTORY[db]()

    if backend_id:
        set_backend_id(backend_id)

    try:
        yield session
        session.commit()
    except OperationalError as error:
        session.rollback()
        log.print_fail(f"Database operation failed: {error}")
        raise
    except Exception:
        session.rollback()
        raise
    finally:
        # source:
        # https://stackoverflow.com/questions/
        # 21078696/why-is-my-scoped-session-raising-an-attributeerror-session-object-has-no-attr
        THREAD_SAFE_SESSION_FACTORY[db].remove()


def is_database_initialized(db: str) -> bool:
    """Check if the database is initialized."""
    global THREAD_SAFE_SESSION_FACTORY  # pylint: disable=global-variable-not-assigned
    return db in THREAD_SAFE_SESSION_FACTORY


def init_database(
    db_name: str,
    db_user: str = "",
    db_password: str = "",
    db_host: str = "localhost",
    db_port: int = 5432,
) -> None:
    log.print_normal(f"Initializing database {db_name} at {db_host}:{db_port}")

    if db_user and db_password:
        uri = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    elif db_user:
        uri = f"postgresql://{db_user}@{db_host}:{db_port}/{db_name}"
    else:
        uri = f"postgresql://{db_host}:{db_port}/{db_name}"

    engine = init_engine(uri, db_name)

    if database_exists(engine.url):
        log.print_normal("Found existing database")
    else:
        log.print_ok_blue("Creating new database!")

    # We need these imports here to avoid circular imports and to ensure
    # that the models have been imported before we create the tables and
    # in the proper order based on relationships.

    # pylint: disable=import-outside-toplevel
    # pylint: disable=unused-import
    # isort: off
    from database.models.user import User  # noqa: F401
    from database.models.author import Author  # noqa: F401
    from database.models.message import Message  # noqa: F401
    from database.models.alert_timing import AlertTiming  # noqa: F401

    # isort: on
    # pylint: enable=import-outside-toplevel
    # pylint: enable=unused-import

    try:
        Base.metadata.create_all(bind=engine)

        _init_session_factory(db_name)
    except OperationalError as exc:
        log.print_fail(f"Failed to initialize database: {exc}")
        log.print_normal("Continuing without db connection...")

import contextlib
import importlib
import pkgutil
import typing as T

from ryutils import log
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import DeclarativeBase, scoped_session, sessionmaker
from sqlalchemy.orm.scoping import ScopedSession
from sqlalchemy_utils import database_exists
from tenacity import retry, stop_after_attempt, wait_exponential

from ry_pg_utils.config import get_config

ENGINE: T.Dict[str, Engine] = {}
THREAD_SAFE_SESSION_FACTORY: T.Dict[str, ScopedSession] = {}


# Modern SQLAlchemy 2.0 declarative base
class Base(DeclarativeBase):
    """Base class for all SQLAlchemy models."""


def get_table_name(base_name: str, verbose: bool = False) -> str:
    """Get the table name. Verbose mode prints the table name."""
    if verbose:
        print(f"{base_name}")
    return base_name


def init_engine(uri: str, db: str, **kwargs: T.Any) -> Engine:
    global ENGINE  # pylint: disable=global-variable-not-assigned
    if db not in ENGINE:
        # Add pool settings to automatically recycle connections
        default_pool_settings = {
            "pool_recycle": 3600,  # Recycle connections after 1 hour
            "pool_pre_ping": True,  # Enable connection health checks
            "pool_size": 5,  # Maintain a pool of connections
            "max_overflow": 10,  # Allow up to 10 additional connections
        }
        # Update kwargs with defaults if not already set
        for key, value in default_pool_settings.items():
            kwargs.setdefault(key, value)
        ENGINE[db] = create_engine(uri, **kwargs)
    return ENGINE[db]


def get_engine(db: str) -> Engine:
    global ENGINE  # pylint: disable=global-variable-not-assigned
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
    if db in THREAD_SAFE_SESSION_FACTORY:
        del THREAD_SAFE_SESSION_FACTORY[db]


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=lambda e: isinstance(e, (OperationalError, TimeoutError)),
)
def _init_session_factory(db: str) -> ScopedSession:
    """Initialize the THREAD_SAFE_SESSION_FACTORY."""
    global ENGINE, THREAD_SAFE_SESSION_FACTORY  # pylint: disable=global-variable-not-assigned
    if db not in ENGINE:
        raise ValueError(
            "Initialize ENGINE by calling init_engine before calling _init_session_factory!"
        )
    if db not in THREAD_SAFE_SESSION_FACTORY:
        session_factory = sessionmaker(bind=ENGINE[db])
        THREAD_SAFE_SESSION_FACTORY[db] = scoped_session(session_factory)
    return THREAD_SAFE_SESSION_FACTORY[db]


def is_session_factory_initialized() -> bool:
    return bool(THREAD_SAFE_SESSION_FACTORY)


@contextlib.contextmanager
def ManagedSession(  # pylint: disable=invalid-name
    db: T.Optional[str] = None,
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
        # assume we're just using the default db
        db = list(THREAD_SAFE_SESSION_FACTORY.keys())[0]

    if db not in THREAD_SAFE_SESSION_FACTORY:
        if get_config().raise_on_use_before_init:
            raise ValueError(f"Call _init_session_factory for {db} before using ManagedSession!")
        log.print_fail(f"Call _init_session_factory for {db} before using ManagedSession!")
        yield None
        return

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=lambda e: isinstance(e, OperationalError),
    )
    def execute_with_retry(session: ScopedSession) -> T.Iterator[ScopedSession]:
        try:
            yield session
            session.commit()
            session.flush()
        except Exception:
            session.rollback()
            raise

    session = THREAD_SAFE_SESSION_FACTORY[db]()

    try:
        yield from execute_with_retry(session)
    finally:
        # source:
        # https://stackoverflow.com/questions/
        # 21078696/why-is-my-scoped-session-raising-an-attributeerror-session-object-has-no-attr
        THREAD_SAFE_SESSION_FACTORY[db].remove()


def is_database_initialized(db: str) -> bool:
    """Check if the database is initialized."""
    global THREAD_SAFE_SESSION_FACTORY  # pylint: disable=global-variable-not-assigned
    return db in THREAD_SAFE_SESSION_FACTORY


def delete_tables(db: str, tables: T.List[str] | None = None) -> None:
    """Delete tables from the database."""
    # pylint: disable=too-many-nested-blocks
    if tables is None:
        # Walk through all modules to find Table classes
        for module_info in pkgutil.walk_packages():
            try:
                module = importlib.import_module(module_info.name)
                for attr_name in dir(module):
                    attr = getattr(module, attr_name)
                    if isinstance(attr, type) and issubclass(attr, Base):
                        # Drop table for each model class
                        if hasattr(attr, "__table__"):
                            attr.__table__.drop(bind=get_engine(db))  # type: ignore[attr-defined]
            except (ImportError, AttributeError):
                continue
    else:
        # Drop specific tables
        for table in tables:
            Base.metadata.tables[table].drop(bind=get_engine(db))


def _import_models_from_module(module_path: str) -> None:
    """
    Dynamically import all model classes from a given module path.

    This ensures all SQLAlchemy models are registered with Base.metadata
    before creating tables.

    Args:
        module_path: Dot-separated module path (e.g., 'database.models')
    """
    try:
        # Import the base module
        base_module = importlib.import_module(module_path)

        # Get the package path
        if hasattr(base_module, "__path__"):
            package_path = base_module.__path__
        else:
            # It's a single module, not a package
            return

        # Walk through all submodules
        for _, modname, _ in pkgutil.walk_packages(
            path=package_path,
            prefix=f"{module_path}.",
        ):
            try:
                importlib.import_module(modname)
            except Exception as e:  # pylint: disable=broad-except
                log.print_warn(f"Failed to import {modname}: {e}")
                continue

        log.print_ok_blue(f"Imported models from {module_path}")
    except ModuleNotFoundError:
        log.print_warn(f"Models module not found: {module_path}")
    except Exception as e:  # pylint: disable=broad-except
        log.print_warn(f"Error importing models from {module_path}: {e}")


def init_database(
    db_name: str,
    db_user: str = "",
    db_password: str = "",
    db_host: str = "localhost",
    db_port: int = 5432,
    models_module: T.Optional[str] = None,
) -> None:
    """
    Initialize a database connection and create tables.

    Args:
        db_name: Name of the database
        db_user: Database username
        db_password: Database password
        db_host: Database host
        db_port: Database port
        models_module: Optional dot-separated module path to your models
                      (e.g., 'database.models' or 'myapp.db.models').
                      All model classes in this module will be automatically
                      imported to register them with SQLAlchemy.
    """
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

    # Import models if module path provided
    if models_module:
        _import_models_from_module(models_module)

    try:
        Base.metadata.create_all(bind=engine)

        _init_session_factory(db_name)
    except OperationalError as exc:
        log.print_fail(f"Failed to initialize database: {exc}")
        log.print_normal("Continuing without db connection...")

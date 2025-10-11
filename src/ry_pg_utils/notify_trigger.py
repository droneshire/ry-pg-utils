import json
import threading
import time
from contextlib import contextmanager
from typing import Any, Callable, Dict, Generator, Iterator, List, Optional, Set

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ry_pg_utils.connect import get_engine


def create_notify_trigger(
    engine: Engine,
    table_name: str,
    channel_name: Optional[str] = None,
    trigger_name: Optional[str] = None,
    events: Optional[List[str]] = None,
    columns: Optional[List[str]] = None,
) -> None:
    """
    Create a notification trigger on a specified table.

    Args:
        engine: SQLAlchemy engine instance
        table_name: Name of the table to add the trigger to
        channel_name: Name of the notification channel (defaults to table_name)
        trigger_name: Name of the trigger (defaults to f"{table_name}_notify_trigger")
        events: List of events to trigger on (defaults to ['INSERT', 'UPDATE', 'DELETE'])
        columns: List of columns to include in the notification payload. If None, all columns.
    """
    if events is None:
        events = ["INSERT", "UPDATE", "DELETE"]

    # Validate events
    valid_events = {"INSERT", "UPDATE", "DELETE"}
    invalid_events = set(events) - valid_events
    if invalid_events:
        raise ValueError(f"Invalid events: {invalid_events}. Valid events are: {valid_events}")

    channel_name = channel_name or table_name
    trigger_name = trigger_name or f"{table_name}_notify_trigger"

    # Validate requested columns
    if columns is not None:
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = :table_name
                    AND table_schema = current_schema()
                """
                ),
                {"table_name": table_name},
            )
            available_columns = {row[0] for row in result}
            invalid_columns = set(columns) - available_columns
            if invalid_columns:
                raise ValueError(
                    f"Invalid columns: {invalid_columns}. "
                    f"Available columns are: {available_columns}"
                )

    # Build JSON payload expression
    if columns:
        data_builder = (
            "json_build_object("
            + ", ".join(
                f"'{col}', CASE WHEN TG_OP = 'DELETE' THEN OLD.{col} ELSE NEW.{col} END"
                for col in columns
            )
            + ")"
        )
    else:
        data_builder = "row_to_json(CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END)"

    trigger_function = f"""
    CREATE OR REPLACE FUNCTION "{trigger_name}_function"()
    RETURNS TRIGGER AS $$
    BEGIN
        PERFORM pg_notify(
            :channel,
            json_build_object(
                'table', TG_TABLE_NAME,
                'action', TG_OP,
                'data', {data_builder}
            )::text
        );
        RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
    END;
    $$ LANGUAGE plpgsql;
    """

    drop_commands = f"""
    DROP TRIGGER IF EXISTS "{trigger_name}_insert" ON "{table_name}";
    DROP TRIGGER IF EXISTS "{trigger_name}_update" ON "{table_name}";
    DROP TRIGGER IF EXISTS "{trigger_name}_delete" ON "{table_name}";
    DROP FUNCTION IF EXISTS "{trigger_name}_function"();
    """

    with engine.begin() as conn:
        conn.execute(text(drop_commands))
        conn.execute(text(trigger_function), {"channel": channel_name})
        for event in events:
            trigger_sql = f"""
            CREATE TRIGGER "{trigger_name}_{event.lower()}"
            AFTER {event} ON "{table_name}"
            FOR EACH ROW EXECUTE FUNCTION "{trigger_name}_function"();
            """
            conn.execute(text(trigger_sql))


def drop_notify_trigger(
    engine: Engine,
    table_name: str,
    trigger_name: Optional[str] = None,
) -> None:
    """
    Drop a notification trigger and its associated function.

    Args:
        engine: SQLAlchemy engine instance
        table_name: Name of the table the trigger is on
        trigger_name: Name of the trigger (defaults to f"{table_name}_notify_trigger")
    """
    trigger_name = trigger_name or f"{table_name}_notify_trigger"

    drop_commands = f"""
    DROP TRIGGER IF EXISTS "{trigger_name}_insert" ON "{table_name}";
    DROP TRIGGER IF EXISTS "{trigger_name}_update" ON "{table_name}";
    DROP TRIGGER IF EXISTS "{trigger_name}_delete" ON "{table_name}";
    DROP FUNCTION IF EXISTS "{trigger_name}_function"();
    """

    with engine.begin() as conn:
        conn.execute(text(drop_commands))


@contextmanager
def subscribe_to_notifications(
    engine: Engine,
    channel_name: str,
    callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    timeout: float = 60.0,
) -> Iterator[Generator[Dict[str, Any], None, None]]:
    """
    Subscribe to PostgreSQL notifications from a channel. Clean shutdown on exit.

    If a callback is given, notifications are processed in a background thread.
    Otherwise, notifications can be consumed via the yielded generator.
    """
    conn_params = engine.url.translate_connect_args()
    conn = psycopg2.connect(
        dbname=conn_params.get("database"),
        user=conn_params.get("username"),
        password=conn_params.get("password"),
        host=conn_params.get("host"),
        port=conn_params.get("port"),
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    stop_flag = threading.Event()

    def notification_generator() -> Generator[Dict[str, Any], None, None]:
        start_time = time.time()
        with conn.cursor() as cur:
            cur.execute(f'LISTEN "{channel_name}";')
            while not stop_flag.is_set() and (time.time() - start_time) < timeout:
                conn.poll()
                while conn.notifies:
                    notify = conn.notifies.pop()
                    try:
                        notification = json.loads(notify.payload)
                        if callback:
                            callback(notification)
                        else:
                            yield notification
                    except json.JSONDecodeError:
                        continue
                time.sleep(0.1)

    try:
        if callback:
            thread = threading.Thread(target=lambda: list(notification_generator()), daemon=True)
            thread.start()
            yield notification_generator()  # yield generator even in callback mode
            stop_flag.set()
            thread.join(timeout=1.0)
        else:
            yield notification_generator()
    finally:
        if not conn.closed:
            conn.close()


class NotificationListener:
    """
    A class to handle PostgreSQL notifications in the background.

    Example usage:
    ```python
    # Create a listener
    listener = NotificationListener(engine)

    # Create a listener for a specific table
    listener.create_listener(table_name="my_table", channel_name="my_channel")

    # Add a callback for a specific channel
    def handle_changes(notification):
        print(f"Received notification: {notification}")

    listener.add_callback("my_table", handle_changes)

    # Start listening in the background
    listener.start()

    # ... your main application code ...

    # When done, stop the listener
    listener.stop()
    ```
    """

    def __init__(self, db_name: str) -> None:
        self.db_name = db_name
        self._running = False
        self._stop_flag = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._callbacks: Dict[str, Set[Callable[[Dict[str, Any]], None]]] = {}
        self._lock = threading.Lock()

    def add_callback(self, channel_name: str, callback: Callable[[Dict[str, Any]], None]) -> None:
        with self._lock:
            self._callbacks.setdefault(channel_name, set()).add(callback)

    def remove_callback(
        self, channel_name: str, callback: Callable[[Dict[str, Any]], None]
    ) -> None:
        with self._lock:
            callbacks = self._callbacks.get(channel_name)
            if callbacks:
                callbacks.discard(callback)
                if not callbacks:
                    del self._callbacks[channel_name]

    def _process_notification(self, notify: Any) -> None:
        """Process a single notification and execute callbacks."""
        try:
            notification = json.loads(notify.payload)
            with self._lock:
                callbacks = self._callbacks.get(notify.channel, set())
                for cb in callbacks:
                    try:
                        cb(notification)
                    except Exception as e:  # pylint: disable=broad-exception-caught
                        print(f"Error in callback for channel {notify.channel}: {e}")
        except json.JSONDecodeError:
            pass

    def _handle_connection(self, conn: Any, channels: List[str]) -> None:
        """Handle the active connection and process notifications."""
        with conn.cursor() as cur:
            for channel in channels:
                cur.execute(f'LISTEN "{channel}";')

            while not self._stop_flag.is_set():
                conn.poll()
                while conn.notifies:
                    notify = conn.notifies.pop()
                    self._process_notification(notify)
                time.sleep(0.1)

    def _create_connection(self, conn_params: Dict[str, Any]) -> Any:
        """Create and configure a new database connection."""
        conn = psycopg2.connect(
            dbname=conn_params.get("database"),
            user=conn_params.get("username"),
            password=conn_params.get("password"),
            host=conn_params.get("host"),
            port=conn_params.get("port"),
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return conn

    def _listen_loop(self) -> None:
        """Main loop for listening to database notifications."""
        engine = get_engine(self.db_name)
        conn_params = engine.url.translate_connect_args()

        while not self._stop_flag.is_set():
            conn = None
            try:
                conn = self._create_connection(conn_params)
                with self._lock:
                    channels = list(self._callbacks.keys())
                self._handle_connection(conn, channels)
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                print(f"Listener connection error: {e}")
                time.sleep(5)
            finally:
                if conn and not conn.closed:
                    conn.close()

    def start(self) -> None:
        if not self._running:
            self._stop_flag.clear()
            self._running = True
            self._thread = threading.Thread(target=self._listen_loop, daemon=True)
            self._thread.start()

    def stop(self) -> None:
        if self._running:
            self._stop_flag.set()
            if self._thread is not None:
                self._thread.join(timeout=2.0)
            self._thread = None
            self._running = False

    def create_listener(
        self,
        table_name: str,
        channel_name: str,
        columns: Optional[List[str]] = None,
        events: Optional[List[str]] = None,
    ) -> None:
        engine = get_engine(self.db_name)
        create_notify_trigger(
            engine=engine,
            table_name=table_name,
            channel_name=channel_name,
            trigger_name=f"{table_name}_notify_trigger",
            events=events,
            columns=columns,
        )

    def remove_listener(self, table_name: str) -> None:
        engine = get_engine(self.db_name)
        drop_notify_trigger(
            engine=engine,
            table_name=table_name,
            trigger_name=f"{table_name}_notify_trigger",
        )

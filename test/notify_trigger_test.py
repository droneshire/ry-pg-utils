import time
import unittest
from test.container_test_base import PostgresOnlyTestBase
from typing import Any, List

from database.connect import close_engine, init_engine
from database.notify_trigger import (
    NotificationListener,
    create_notify_trigger,
    drop_notify_trigger,
    subscribe_to_notifications,
)
from sqlalchemy import text
from sqlalchemy.engine import Engine


class TestNotifyTrigger(PostgresOnlyTestBase):
    """Test cases for the notification trigger functionality."""

    engine: Engine
    test_table: str
    test_db_name: str = "test_notify_db"

    @classmethod
    def setUpClass(cls) -> None:
        """Set up test fixtures before running any tests."""
        # Call parent setUpClass to start the container
        super().setUpClass()

        # Get connection parameters from the container
        conn_params = cls.get_postgres_connection_params()

        # Create connection URI and register engine with database.connect module
        connection_uri = (
            f"postgresql://{conn_params['user']}:{conn_params['password']}@"
            f"{conn_params['host']}:{conn_params['port']}/{conn_params['dbname']}"
        )
        cls.engine = init_engine(connection_uri, cls.test_db_name)
        cls.test_table = "test_table"

        # Create test table
        with cls.engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {cls.test_table}"))
            conn.execute(
                text(
                    f"""
                CREATE TABLE {cls.test_table} (
                    id SERIAL PRIMARY KEY,
                    name TEXT,
                    value INTEGER
                )
            """
                )
            )
            conn.commit()

    @classmethod
    def tearDownClass(cls) -> None:
        """Clean up after all tests are done."""
        # Drop test table
        with cls.engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {cls.test_table}"))
            conn.commit()

        # Close engine and clean up from database.connect registry
        close_engine(cls.test_db_name)

        # Call parent tearDownClass to stop the container
        super().tearDownClass()

    def setUp(self) -> None:
        """Set up test fixtures before each test method."""
        # Drop any existing triggers
        drop_notify_trigger(self.engine, self.test_table)

    def tearDown(self) -> None:
        """Clean up after each test method."""
        # Drop any triggers created during the test
        drop_notify_trigger(self.engine, self.test_table)

    def test_create_notify_trigger(self) -> None:
        """Test creating a notification trigger."""
        # Create trigger
        create_notify_trigger(self.engine, self.test_table)

        # Verify trigger was created
        with self.engine.connect() as conn:
            result = conn.execute(
                text(
                    """
                SELECT trigger_name
                FROM information_schema.triggers
                WHERE event_object_table = :table_name
            """
                ),
                {"table_name": self.test_table},
            )
            triggers: List[str] = [row[0] for row in result]
            self.assertIn(f"{self.test_table}_notify_trigger_insert", triggers)
            self.assertIn(f"{self.test_table}_notify_trigger_update", triggers)
            self.assertIn(f"{self.test_table}_notify_trigger_delete", triggers)

    def test_drop_notify_trigger(self) -> None:
        """Test dropping a notification trigger."""
        # Create trigger first
        create_notify_trigger(self.engine, self.test_table)

        # Drop trigger
        drop_notify_trigger(self.engine, self.test_table)

        # Verify trigger was dropped
        with self.engine.connect() as conn:
            result = conn.execute(
                text(
                    """
                SELECT trigger_name
                FROM information_schema.triggers
                WHERE event_object_table = :table_name
            """
                ),
                {"table_name": self.test_table},
            )
            triggers: List[str] = [row[0] for row in result]
            self.assertEqual(len(triggers), 0)

    def test_notification_listener(self) -> None:
        """Test the NotificationListener class."""
        # Create trigger first
        create_notify_trigger(self.engine, self.test_table)

        listener = NotificationListener(self.test_db_name)

        # Create a list to store received notifications
        received_notifications: List[dict[str, Any]] = []

        def notification_callback(notification: dict[str, Any]) -> None:
            received_notifications.append(notification)

        # Create listener and add callback
        listener.create_listener(self.test_table, self.test_table)  # Use table name as channel
        listener.add_callback(self.test_table, notification_callback)

        # Start listening
        listener.start()
        time.sleep(1)  # Give time for listener to start

        try:
            # Insert test data
            with self.engine.connect() as conn:
                conn.execute(
                    text(
                        """
                    INSERT INTO test_table (name, value)
                    VALUES ('test', 100)
                """
                    )
                )
                conn.commit()

            # Wait for notification with retries
            max_retries = 10  # Increased retries
            retry_count = 0
            while len(received_notifications) == 0 and retry_count < max_retries:
                time.sleep(1.0)  # Wait longer between retries
                retry_count += 1

            # Verify notification was received
            self.assertGreater(
                len(received_notifications), 0, "No notifications received after all retries"
            )
            notification = received_notifications[0]
            self.assertEqual(notification["table"], self.test_table)
            self.assertEqual(notification["action"], "INSERT")
            self.assertEqual(notification["data"]["name"], "test")
            self.assertEqual(notification["data"]["value"], 100)

        finally:
            listener.stop()

    def test_subscribe_to_notifications(self) -> None:
        """Test the subscribe_to_notifications context manager."""
        # Create trigger
        create_notify_trigger(self.engine, self.test_table)

        # Create a list to store received notifications
        received_notifications: List[dict[str, Any]] = []

        def notification_callback(notification: dict[str, Any]) -> None:
            received_notifications.append(notification)

        # Subscribe to notifications
        with subscribe_to_notifications(
            self.engine, self.test_table, callback=notification_callback
        ):
            # Give time for subscription to be established
            time.sleep(1)

            # Insert test data
            with self.engine.connect() as conn:
                conn.execute(
                    text(
                        """
                    INSERT INTO test_table (name, value)
                    VALUES ('test', 100)
                """
                    )
                )
                conn.commit()

            # Wait for notification with retries
            max_retries = 10  # Increased retries
            retry_count = 0
            while len(received_notifications) == 0 and retry_count < max_retries:
                time.sleep(1.0)  # Wait longer between retries
                retry_count += 1

            # Verify notification was received
            self.assertGreater(
                len(received_notifications), 0, "No notifications received after all retries"
            )
            notification = received_notifications[0]
            self.assertEqual(notification["table"], self.test_table)
            self.assertEqual(notification["action"], "INSERT")
            self.assertEqual(notification["data"]["name"], "test")
            self.assertEqual(notification["data"]["value"], 100)

    def test_invalid_events(self) -> None:
        """Test creating trigger with invalid events."""
        with self.assertRaises(ValueError):
            create_notify_trigger(self.engine, self.test_table, events=["INVALID_EVENT"])

    def test_invalid_columns(self) -> None:
        """Test creating trigger with invalid columns."""
        with self.assertRaises(ValueError):
            create_notify_trigger(self.engine, self.test_table, columns=["non_existent_column"])


if __name__ == "__main__":
    unittest.main()

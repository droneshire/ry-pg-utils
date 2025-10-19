"""
Test database connection handling and resource cleanup.

This test verifies that database connections are properly closed
to avoid ResourceWarning about unclosed sockets.
"""

import gc
import typing as T
import unittest
import warnings
from test.postgres_test_base import PostgresOnlyTestBase

import psycopg2
from google.protobuf.timestamp_pb2 import Timestamp  # pylint: disable=no-name-in-module
from sqlalchemy import Column, Integer, String, text

from ry_pg_utils.connect import Base, close_engine, get_engine, init_database
from ry_pg_utils.pb_types.database_pb2 import (  # pylint: disable=no-name-in-module
    DatabaseNotificationPb,
    PostgresMessagePb,
    PostgresPb,
)

# Suppress resource warnings from external libraries at module level
warnings.filterwarnings("ignore", category=ResourceWarning)


# Test model using SQLAlchemy
class TestMessage(Base):
    """Test message model for database tests."""

    __tablename__ = "test_messages"

    id = Column(Integer, primary_key=True, autoincrement=True)
    content = Column(String(500))
    author = Column(String(100))
    source_id = Column(String(100))


class DatabaseConnectionTest(PostgresOnlyTestBase):
    """Test database connection handling and cleanup."""

    cursor: T.Optional[psycopg2.extensions.cursor] = None
    connection: T.Optional[psycopg2.extensions.connection] = None
    db_name: str = ""
    host: str = ""
    port: int = 0
    user: str = ""
    password: str = ""

    @classmethod
    def tearDownClass(cls) -> None:
        """Ensure all connections are closed at class level."""
        try:
            # Force close any remaining connections
            gc.collect()
        finally:
            super().tearDownClass()

    @classmethod
    def setUpClass(cls) -> None:
        """Set up class with warning suppression."""
        # Suppress resource warnings from external libraries
        warnings.filterwarnings("ignore", category=ResourceWarning, module="ry_pg_utils")
        warnings.filterwarnings("ignore", category=ResourceWarning, module="psycopg2")
        super().setUpClass()

    def set_up_db(self) -> None:
        """Set up test database with proper connection handling."""
        # Get connection parameters from the parent container test base class
        params = self.get_postgres_connection_params()
        self.host = params["host"]
        self.port = int(params["port"])
        self.user = params["user"]
        self.password = params["password"]
        self.db_name = "test_db"

        # Connect to the default database first to create test database
        self.connection = None
        self.cursor = None

        try:
            self.connection = psycopg2.connect(
                dbname=params["dbname"],  # Use container's default database
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
            )
            self.connection.autocommit = True
            self.cursor = self.connection.cursor()

            # Drop the test database if it exists from a previous run
            try:
                self.cursor.execute(f"DROP DATABASE IF EXISTS {self.db_name};")
            except Exception:  # pylint: disable=broad-exception-caught
                pass

            # Create the test database
            try:
                query = f'CREATE DATABASE "{self.db_name}";'
                self.cursor.execute(query)
            except psycopg2.errors.DuplicateDatabase:  # pylint: disable=no-member
                pass

            # Initialize the test database with SQLAlchemy
            init_database(
                db_host=self.host,
                db_port=self.port,
                db_name=self.db_name,
                db_user=self.user,
                db_password=self.password,
            )

            # Create test tables
            engine = get_engine(self.db_name)
            Base.metadata.create_all(bind=engine)

        except Exception:
            # Clean up on error
            if self.cursor is not None and not self.cursor.closed:
                try:
                    self.cursor.close()
                except Exception:  # pylint: disable=broad-exception-caught
                    pass
            if self.connection is not None and not self.connection.closed:
                try:
                    self.connection.close()
                except Exception:  # pylint: disable=broad-exception-caught
                    pass
            raise

    def tear_down_db(self) -> None:
        """Clean up test database with proper connection handling."""
        # Close engine and all connections to the test database
        try:
            close_engine(self.db_name)
        except (psycopg2.Error, RuntimeError, KeyError):
            pass  # Ignore errors during engine closure

        if self.cursor:
            try:
                if not self.cursor.closed:
                    self.cursor.close()
            except (psycopg2.Error, psycopg2.InterfaceError):
                pass
        self.cursor = None

        if self.connection:
            try:
                if not self.connection.closed:
                    self.connection.close()
            except (psycopg2.Error, psycopg2.InterfaceError):
                pass
        self.connection = None

        # Force garbage collection to clean up any remaining connections
        gc.collect()

        # Reconnect to the default database to drop the test database
        cleanup_conn = None
        cleanup_cursor = None
        try:
            params = self.get_postgres_connection_params()
            cleanup_conn = psycopg2.connect(
                dbname=params["dbname"],
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
            )
            cleanup_conn.autocommit = True
            cleanup_cursor = cleanup_conn.cursor()

            # Drop the test database
            cleanup_cursor.execute(f"DROP DATABASE IF EXISTS {self.db_name};")
        except Exception:  # pylint: disable=broad-exception-caught
            pass
        finally:
            if cleanup_cursor:
                try:
                    cleanup_cursor.close()
                except (psycopg2.Error, psycopg2.InterfaceError):
                    pass
            if cleanup_conn:
                try:
                    cleanup_conn.close()
                except (psycopg2.Error, psycopg2.InterfaceError):
                    pass

        # Final garbage collection after cleanup
        gc.collect()

    def test_postgres_pb_creation(self) -> None:
        """Test creating and populating PostgresPb protobuf message."""
        postgres_pb = PostgresPb()
        postgres_pb.database = "test_db"
        postgres_pb.user = "test_user"
        postgres_pb.password = "test_password"
        postgres_pb.host = "localhost"
        postgres_pb.port = 5432
        postgres_pb.backendId = "backend_1"

        self.assertEqual(postgres_pb.database, "test_db")
        self.assertEqual(postgres_pb.user, "test_user")
        self.assertEqual(postgres_pb.password, "test_password")
        self.assertEqual(postgres_pb.host, "localhost")
        self.assertEqual(postgres_pb.port, 5432)
        self.assertEqual(postgres_pb.backendId, "backend_1")

    def test_postgres_message_pb_creation(self) -> None:
        """Test creating PostgresMessagePb with timestamp."""
        message_pb = PostgresMessagePb()
        timestamp = Timestamp()
        timestamp.GetCurrentTime()
        message_pb.utime.CopyFrom(timestamp)

        postgres_pb = PostgresPb()
        postgres_pb.database = "test_db"
        message_pb.postgres.CopyFrom(postgres_pb)

        self.assertEqual(message_pb.postgres.database, "test_db")
        self.assertGreater(message_pb.utime.seconds, 0)

    def test_database_notification_pb_creation(self) -> None:
        """Test creating DatabaseNotificationPb."""
        notification_pb = DatabaseNotificationPb()
        notification_pb.table_name = "test_table"
        notification_pb.channel_name = "test_channel"
        notification_pb.action = "INSERT"
        notification_pb.payload = '{"id": 1, "name": "test"}'

        timestamp = Timestamp()
        timestamp.GetCurrentTime()
        notification_pb.utime.CopyFrom(timestamp)

        self.assertEqual(notification_pb.table_name, "test_table")
        self.assertEqual(notification_pb.channel_name, "test_channel")
        self.assertEqual(notification_pb.action, "INSERT")
        self.assertIn("id", notification_pb.payload)

    def test_database_connection_cleanup(self) -> None:
        """Test that database connections are properly cleaned up."""
        self.set_up_db()
        try:
            # Create a direct connection to test cleanup
            test_conn = psycopg2.connect(
                dbname=self.db_name,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
            )

            # Do some work
            with test_conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                self.assertIsNotNone(result)
                self.assertEqual(result[0], 1)  # type: ignore

            # Ensure connection is closed
            test_conn.close()
            self.assertTrue(test_conn.closed)

        finally:
            self.tear_down_db()

    def test_sqlalchemy_model_operations(self) -> None:
        """Test SQLAlchemy model operations with proper cleanup."""
        self.set_up_db()
        try:
            engine = get_engine(self.db_name)

            # Explicitly create the test_messages table
            TestMessage.__table__.create(bind=engine, checkfirst=True)  # type: ignore[attr-defined]

            # Insert test data
            with engine.connect() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO test_messages (content, author, source_id)
                        VALUES ('Hello World', 'test_author', 'source_1')
                    """
                    )
                )
                conn.commit()

            # Query test data
            with engine.connect() as conn:
                result = conn.execute(text("SELECT * FROM test_messages"))
                rows = result.fetchall()
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0][1], "Hello World")  # content column

        finally:
            self.tear_down_db()


if __name__ == "__main__":
    unittest.main()

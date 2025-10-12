"""Unit tests for the db_query module."""

import os
import tempfile
import unittest
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import psycopg2

from ry_pg_utils.tools.db_query import DbQuery, DbQueryPsycopg2, DbQuerySpark
from test.postgres_test_base import PostgresOnlyTestBase


class TestDbQueryPsycopg2(PostgresOnlyTestBase):
    """Test cases for DbQueryPsycopg2 class."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        params = self.get_postgres_connection_params()

        # Create a test table
        self.conn = psycopg2.connect(
            host=params["host"],
            port=params["port"],
            dbname=params["dbname"],
            user=params["user"],
            password=params["password"],
        )
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS test_users (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100),
                    email VARCHAR(200),
                    age INTEGER
                )
                """
            )
            cursor.execute(
                """
                INSERT INTO test_users (name, email, age) VALUES
                ('Alice', 'alice@example.com', 30),
                ('Bob', 'bob@example.com', 25),
                ('Charlie', 'charlie@example.com', 35)
                """
            )
            self.conn.commit()

        self.db_query = DbQueryPsycopg2(
            postgres_host=params["host"],
            postgres_port=int(params["port"]),
            postgres_database=params["dbname"],
            postgres_user=params["user"],
            postgres_password=params["password"],
            is_local=False,
        )

    def tearDown(self) -> None:
        """Clean up after tests."""
        if hasattr(self, "conn") and self.conn:
            with self.conn.cursor() as cursor:
                cursor.execute("DROP TABLE IF EXISTS test_users")
                self.conn.commit()
            self.conn.close()

        if hasattr(self, "db_query") and self.db_query.conn:
            self.db_query.close()

    def test_initialization(self) -> None:
        """Test DbQueryPsycopg2 initialization."""
        self.assertIsNotNone(self.db_query.postgres_host)
        self.assertIsNotNone(self.db_query.postgres_port)
        self.assertIsNotNone(self.db_query.postgres_database)
        self.assertIsNotNone(self.db_query.postgres_user)
        self.assertIsNotNone(self.db_query.postgres_password)
        self.assertFalse(self.db_query.is_local)
        self.assertIsNone(self.db_query.conn)

    def test_connect(self) -> None:
        """Test database connection."""
        self.db_query.connect(use_ssh_tunnel=False)
        self.assertIsNotNone(self.db_query.conn)
        self.assertIsInstance(self.db_query.conn, psycopg2.extensions.connection)

    def test_query(self) -> None:
        """Test executing a query."""
        self.db_query.connect(use_ssh_tunnel=False)
        df = self.db_query.query("SELECT * FROM test_users ORDER BY id")

        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 3)
        self.assertListEqual(list(df.columns), ["id", "name", "email", "age"])
        self.assertEqual(df.iloc[0]["name"], "Alice")
        self.assertEqual(df.iloc[1]["name"], "Bob")
        self.assertEqual(df.iloc[2]["name"], "Charlie")

    def test_query_with_filter(self) -> None:
        """Test executing a query with WHERE clause."""
        self.db_query.connect(use_ssh_tunnel=False)
        df = self.db_query.query("SELECT * FROM test_users WHERE age > 28 ORDER BY name")

        self.assertEqual(len(df), 2)
        self.assertEqual(df.iloc[0]["name"], "Alice")
        self.assertEqual(df.iloc[1]["name"], "Charlie")

    def test_load_tables(self) -> None:
        """Test loading multiple tables."""
        self.db_query.connect(use_ssh_tunnel=False)
        tables = ['"public"."test_users"']
        dfs = self.db_query.load_tables(tables)

        self.assertIsInstance(dfs, dict)
        self.assertIn('"public"."test_users"', dfs)
        self.assertEqual(len(dfs['"public"."test_users"']), 3)

    def test_clear_table(self) -> None:
        """Test clearing a table."""
        self.db_query.connect(use_ssh_tunnel=False)

        # Verify data exists
        df = self.db_query.query("SELECT COUNT(*) as count FROM test_users")
        self.assertEqual(df.iloc[0]["count"], 3)

        # Clear the table
        self.db_query.clear('"public"."test_users"')

        # Verify table is empty
        df = self.db_query.query("SELECT COUNT(*) as count FROM test_users")
        self.assertEqual(df.iloc[0]["count"], 0)

    def test_db_static_method(self) -> None:
        """Test the db static method for table name formatting."""
        table_name = DbQuery.db("test_table")
        self.assertEqual(table_name, '"public"."test_table"')

    def test_close_connection(self) -> None:
        """Test closing database connection."""
        self.db_query.connect(use_ssh_tunnel=False)
        self.assertIsNotNone(self.db_query.conn)

        self.db_query.close()
        self.assertIsNone(self.db_query.conn)

    def test_query_verbose(self) -> None:
        """Test query with verbose output."""
        self.db_query.connect(use_ssh_tunnel=False)
        df = self.db_query.query("SELECT * FROM test_users", verbose=True)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 3)

    def test_query_error_handling(self) -> None:
        """Test query error handling."""
        self.db_query.connect(use_ssh_tunnel=False)
        df = self.db_query.query("SELECT * FROM nonexistent_table")
        # Should return empty DataFrame on error
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 0)


class TestDbQueryInitialization(unittest.TestCase):
    """Test cases for DbQuery initialization with various parameters."""

    def test_init_with_custom_params(self) -> None:
        """Test initialization with custom parameters."""
        db_query = DbQueryPsycopg2(
            postgres_host="custom_host",
            postgres_port=5433,
            postgres_database="custom_db",
            postgres_user="custom_user",
            postgres_password="custom_pass",
            is_local=True,
        )

        self.assertEqual(db_query.postgres_host, "custom_host")
        self.assertEqual(db_query.postgres_port, 5433)
        self.assertEqual(db_query.postgres_database, "custom_db")
        self.assertEqual(db_query.postgres_user, "custom_user")
        self.assertEqual(db_query.postgres_password, "custom_pass")
        self.assertTrue(db_query.is_local)
        self.assertEqual(db_query.db_name, "temp_custom_db")

    @patch("ry_pg_utils.tools.db_query.config")
    def test_init_with_config_fallback(self, mock_config: Mock) -> None:
        """Test initialization falls back to config when parameters not provided."""
        mock_config.pg_config.postgres_host = "config_host"
        mock_config.pg_config.postgres_port = 5432
        mock_config.pg_config.postgres_db = "config_db"
        mock_config.pg_config.postgres_user = "config_user"
        mock_config.pg_config.postgres_password = "config_pass"

        db_query = DbQueryPsycopg2()

        self.assertEqual(db_query.postgres_host, "config_host")
        self.assertEqual(db_query.postgres_port, 5432)
        self.assertEqual(db_query.postgres_database, "config_db")
        self.assertEqual(db_query.postgres_user, "config_user")
        self.assertEqual(db_query.postgres_password, "config_pass")

    def test_postgres_uri_construction(self) -> None:
        """Test PostgreSQL URI construction."""
        db_query = DbQueryPsycopg2(
            postgres_host="localhost",
            postgres_port=5432,
            postgres_database="testdb",
            postgres_user="testuser",
            postgres_password="testpass",
        )

        expected_uri = "postgresql://testuser:testpass@localhost:5432/testdb"
        self.assertEqual(db_query.postgres_uri, expected_uri)


class TestDbQuerySSH(unittest.TestCase):
    """Test cases for SSH-related functionality."""

    @patch("ry_pg_utils.tools.db_query.paramiko.SSHClient")
    @patch("ry_pg_utils.tools.db_query.modern_ssh_tunnel.SSHTunnelForwarder")
    def test_establish_ssh_tunnel(self, mock_tunnel: Mock, _mock_ssh_client: Mock) -> None:
        """Test establishing SSH tunnel."""
        db_query = DbQueryPsycopg2(
            postgres_host="remote_host",
            postgres_port=5432,
            postgres_database="testdb",
            postgres_user="testuser",
            postgres_password="testpass",
            ssh_host="ssh_host",
            ssh_port=22,
            ssh_user="ssh_user",
            ssh_pkey="/path/to/key",
        )

        # Mock the tunnel
        mock_tunnel_instance = MagicMock()
        mock_tunnel_instance.is_active = True
        mock_tunnel.return_value = mock_tunnel_instance

        db_query._establish_ssh_tunnel()  # pylint: disable=protected-access

        mock_tunnel.assert_called_once_with(
            ("ssh_host", 22),
            ssh_username="ssh_user",
            ssh_pkey="/path/to/key",
            remote_bind_address=("127.0.0.1", 5432),
        )
        mock_tunnel_instance.start.assert_called_once()
        self.assertIsNotNone(db_query.ssh_tunnel)

    def test_ssh_tunnel_requires_parameters(self) -> None:
        """Test that SSH tunnel requires all necessary parameters."""
        db_query = DbQueryPsycopg2(
            postgres_host="remote_host",
            postgres_port=5432,
            postgres_database="testdb",
            postgres_user="testuser",
            postgres_password="testpass",
            # Missing SSH parameters
        )

        with self.assertRaises(AssertionError) as context:
            db_query._establish_ssh_tunnel()  # pylint: disable=protected-access

        self.assertIn("SSH host is required", str(context.exception))


class TestDbQuerySpark(unittest.TestCase):
    """Test cases for DbQuerySpark class."""

    @patch("ry_pg_utils.tools.db_query.SparkSession")
    def test_spark_initialization(self, mock_spark: Mock) -> None:
        """Test Spark DbQuery initialization."""
        mock_spark_builder = MagicMock()
        mock_spark.builder = mock_spark_builder
        mock_spark_builder.appName.return_value = mock_spark_builder
        mock_spark_builder.config.return_value = mock_spark_builder
        mock_spark_builder.getOrCreate.return_value = MagicMock()

        db_query = DbQuerySpark(
            postgres_host="localhost",
            postgres_port=5432,
            postgres_database="testdb",
            postgres_user="testuser",
            postgres_password="testpass",
        )

        self.assertIsNotNone(db_query.connection_properties)
        self.assertEqual(db_query.connection_properties["user"], "testuser")
        self.assertEqual(db_query.connection_properties["password"], "testpass")
        self.assertEqual(db_query.connection_properties["driver"], "org.postgresql.Driver")
        self.assertIn("jdbc:postgresql://", db_query.jdbc_url)

    @patch("ry_pg_utils.tools.db_query.SparkSession")
    def test_spark_connect_no_tunnel(self, mock_spark: Mock) -> None:
        """Test Spark connection without SSH tunnel."""
        mock_spark_builder = MagicMock()
        mock_spark.builder = mock_spark_builder
        mock_spark_builder.appName.return_value = mock_spark_builder
        mock_spark_builder.config.return_value = mock_spark_builder
        mock_spark_builder.getOrCreate.return_value = MagicMock()

        db_query = DbQuerySpark(
            postgres_host="localhost",
            postgres_port=5432,
            postgres_database="testdb",
            postgres_user="testuser",
            postgres_password="testpass",
        )

        db_query.connect(use_ssh_tunnel=False)
        self.assertIn("localhost", db_query.jdbc_url)
        self.assertIn("5432", db_query.jdbc_url)


class TestDbQueryCopyLocal(unittest.TestCase):
    """Test cases for copying database locally."""

    @patch("ry_pg_utils.tools.db_query.paramiko.SSHClient")
    @patch("ry_pg_utils.tools.db_query.os.path.exists")
    @patch("ry_pg_utils.tools.db_query.time.time")
    @patch("ry_pg_utils.tools.db_query.os.path.getmtime")
    def test_maybe_copy_database_skip_recent(
        self,
        mock_getmtime: Mock,
        mock_time: Mock,
        mock_exists: Mock,
        mock_ssh: Mock,
    ) -> None:
        """Test that database copy is skipped if file is recent."""
        mock_exists.return_value = True
        mock_time.return_value = 1000.0
        mock_getmtime.return_value = 950.0  # 50 seconds ago

        db_query = DbQueryPsycopg2(
            postgres_host="localhost",
            postgres_port=5432,
            postgres_database="testdb",
            postgres_user="testuser",
            postgres_password="testpass",
            ssh_host="ssh_host",
            ssh_port=22,
            ssh_user="ssh_user",
            ssh_pkey="/path/to/key",
        )

        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            local_path = tmp.name

        try:
            # pylint: disable=protected-access
            db_query._maybe_copy_database_locally(local_path)
            # Should not establish SSH connection since file is recent
            mock_ssh.assert_not_called()
        finally:
            if os.path.exists(local_path):
                os.remove(local_path)

    @patch("ry_pg_utils.tools.db_query.subprocess.run")
    def test_import_local_database_sets_pgpassword(self, mock_run: Mock) -> None:
        """Test that import_local_database sets PGPASSWORD environment variable."""
        db_query = DbQueryPsycopg2(
            postgres_host="localhost",
            postgres_port=5432,
            postgres_database="testdb",
            postgres_user="testuser",
            postgres_password="testpass",
        )

        # Mock subprocess to prevent actual execution
        mock_run.return_value = MagicMock(stdout=b"", stderr=b"")

        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            local_path = tmp.name

        try:
            # pylint: disable=protected-access
            db_query._import_local_database(local_path, "temp_testdb")
            self.assertEqual(os.environ.get("PGPASSWORD"), "testpass")
        finally:
            if os.path.exists(local_path):
                os.remove(local_path)
            # Clean up environment variable
            if "PGPASSWORD" in os.environ:
                del os.environ["PGPASSWORD"]


class TestDbQueryRunCommand(unittest.TestCase):
    """Test cases for running remote commands."""

    @patch("ry_pg_utils.tools.db_query.paramiko.SSHClient")
    def test_run_command(self, mock_ssh_client: Mock) -> None:
        """Test running a command on remote server."""
        mock_client_instance = MagicMock()
        mock_ssh_client.return_value = mock_client_instance

        # Mock exec_command return values
        mock_stdin = MagicMock()
        mock_stdout = MagicMock()
        mock_stderr = MagicMock()
        mock_stdout.read.return_value = b"command output"
        mock_stderr.read.return_value = b""
        mock_client_instance.exec_command.return_value = (mock_stdin, mock_stdout, mock_stderr)

        db_query = DbQueryPsycopg2(
            postgres_host="localhost",
            postgres_port=5432,
            postgres_database="testdb",
            postgres_user="testuser",
            postgres_password="testpass",
            ssh_host="ssh_host",
            ssh_port=22,
            ssh_user="ssh_user",
            ssh_pkey="/path/to/key",
        )

        db_query.run_command("ls -la")

        mock_client_instance.connect.assert_called_once_with(
            hostname="ssh_host",
            port=22,
            username="ssh_user",
            key_filename="/path/to/key",
        )
        mock_client_instance.exec_command.assert_called_once_with("ls -la")
        mock_client_instance.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()

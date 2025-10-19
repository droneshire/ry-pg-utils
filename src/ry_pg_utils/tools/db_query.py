import abc
import concurrent.futures
import os
import subprocess
import time
import typing as T

import pandas as pd
import paramiko
import psycopg2
from pyspark.sql import SparkSession
from ryutils import log, modern_ssh_tunnel

from ry_pg_utils.config import get_config


class DbQuery(abc.ABC):
    DB_STALE_MINS = 15
    LOCAL_HOST = "127.0.0.1"

    # pylint: disable=too-many-arguments
    # pylint: disable=too-many-positional-arguments
    def __init__(
        self,
        postgres_host: str | None = None,
        postgres_port: int | None = None,
        postgres_database: str | None = None,
        postgres_user: str | None = None,
        postgres_password: str | None = None,
        postrgres_url: str | None = None,
        ssh_host: str | None = None,
        ssh_port: int | None = None,
        ssh_user: str | None = None,
        ssh_pkey: str | None = None,
        is_local: bool = False,
        verbose: bool = False,
    ):
        self.is_local = is_local

        self.postgres_host = (
            postgres_host if postgres_host is not None else get_config().postgres_host
        )
        self.postgres_port = (
            postgres_port if postgres_port is not None else get_config().postgres_port
        )
        self.postgres_database = (
            postgres_database if postgres_database is not None else get_config().postgres_db
        )
        self.postgres_user = (
            postgres_user if postgres_user is not None else get_config().postgres_user
        )
        self.postgres_password = (
            postgres_password if postgres_password is not None else get_config().postgres_password
        )
        self.postgres_uri = (
            f"postgresql://{self.postgres_user}:{self.postgres_password}@"
            f"{self.postgres_host}:{self.postgres_port}/{self.postgres_database}"
        )
        self.postrgres_url = postrgres_url if postrgres_url is not None else self.postgres_uri

        self.ssh_host = ssh_host if ssh_host is not None else get_config().ssh_host
        self.ssh_port = ssh_port if ssh_port is not None else get_config().ssh_port
        self.ssh_user = ssh_user if ssh_user is not None else get_config().ssh_user
        self.ssh_pkey = ssh_pkey if ssh_pkey is not None else get_config().ssh_key_path

        self.db_name = f"temp_{self.postgres_database}" if is_local else self.postgres_database

        if verbose:
            log.print_normal(ssh_host, ssh_port, ssh_user, ssh_pkey)
            log.print_normal(
                postgres_host,
                postgres_port,
                postgres_database,
                postgres_user,
                postgres_password,
                postrgres_url,
                ssh_host,
                ssh_port,
                ssh_user,
                ssh_pkey,
            )

        self.ssh_tunnel: modern_ssh_tunnel.SSHTunnelForwarder | None = None
        self.conn: psycopg2.extensions.connection | SparkSession | None = None

    def __del__(self) -> None:
        """Ensure connections are closed when object is deleted."""
        try:
            self.close()
        except Exception:  # pylint: disable=broad-except
            pass

    @staticmethod
    def db(table_name: str) -> str:
        return f'"public"."{table_name}"'

    def _maybe_copy_database_locally(self, local_db_path: str) -> None:
        # Define the path to the local copy of the database

        # Check if the local database file was modified in the last 5 minutes
        if os.path.exists(local_db_path):
            last_modified_time = os.path.getmtime(local_db_path)
            current_time = time.time()
            if current_time - last_modified_time < 60.0 * self.DB_STALE_MINS:
                log.print_normal(
                    f"Local database copy was modified in the last "
                    f"{self.DB_STALE_MINS} minutes. Skipping copy."
                )
                return

        # Copy the database locally
        log.print_normal("Copying database locally...")

        # Validate required parameters
        assert self.ssh_host is not None, "SSH host is required"
        assert self.ssh_port is not None, "SSH port is required"
        assert self.ssh_user is not None, "SSH user is required"
        assert self.ssh_pkey is not None, "SSH private key path is required"

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            self.ssh_host,
            port=self.ssh_port,
            username=self.ssh_user,
            key_filename=self.ssh_pkey,
        )
        sftp = client.open_sftp()

        remote_temp_file_path = "/tmp/backup_file"
        command = (
            f"PGPASSWORD={self.postgres_password} pg_dump -U {self.postgres_user} "
            f"-h {self.postgres_host} -p {self.postgres_port} -F c -b -v -f "
            f"{remote_temp_file_path} {self.postgres_database}"
        )
        log.print_normal(f"Running command: PGPASSWORD=**** {' '.join(command.split()[1:])}")

        # Run pg_dump on the remote server
        _, stdout, stderr = client.exec_command(command)
        log.print_normal(stdout.read().decode())
        log.print_normal(stderr.read().decode())

        # SFTP setup
        sftp.get(remote_temp_file_path, local_db_path)
        sftp.close()
        client.close()

    def _import_local_database(self, local_db_path: str, temp_db_name: str) -> None:
        assert self.postgres_password is not None, "Postgres password is required"
        os.environ["PGPASSWORD"] = self.postgres_password

        # Check if the temporary database already exists
        check_db_exists_command = (
            f"psql -U {self.postgres_user} -h {self.postgres_host} "
            f"-p {self.postgres_port} -lqt | cut -d \\| -f 1 | grep -w {temp_db_name}"
        )
        log.print_normal(
            f"Checking if database {temp_db_name} already exists...\n{check_db_exists_command}"
        )
        result = subprocess.run(
            check_db_exists_command, shell=True, check=True, stdout=subprocess.PIPE
        )

        drop_db_command = (
            f"psql -U {self.postgres_user} -h {self.postgres_host} "
            f"-p {self.postgres_port} -c 'DROP DATABASE {temp_db_name}'"
        )
        if result.stdout or result.stderr:
            log.print_normal(f"Database {temp_db_name} already exists. Dropping it first...")
            subprocess.run(drop_db_command, shell=True, check=True)

        log.print_normal("Creating temporary database...")
        create_db_command = (
            f"psql -U {self.postgres_user} -h {self.postgres_host} "
            f"-p {self.postgres_port} -c 'CREATE DATABASE {temp_db_name}'"
        )
        try:
            subprocess.run(drop_db_command, shell=True, check=True)
        except Exception:  # pylint: disable=broad-except
            pass

        subprocess.run(create_db_command, shell=True, check=True)

        log.print_normal("Restoring database into the temporary database...")

        # # Restore the database from the binary dump file using pg_restore
        restore_db_command = (
            f"pg_restore -U {self.postgres_user} -h {self.postgres_host} "
            f"-p {self.postgres_port} -d {temp_db_name} -v {local_db_path}"
        )
        subprocess.run(restore_db_command, shell=True, check=True)

        log.print_normal("Database import complete!")

        log.print_normal(f"Database imported into temporary database {temp_db_name}")

    @abc.abstractmethod
    def connect(self, use_ssh_tunnel: bool = False) -> None:
        pass

    @abc.abstractmethod
    def load_tables(self, tables: T.List[str]) -> T.Dict[str, pd.DataFrame]:
        pass

    def _establish_ssh_tunnel(self) -> None:
        log.print_normal(
            "Establishing SSH tunnel: ",
            self.ssh_host,
            self.ssh_port,
            self.ssh_user,
            self.ssh_pkey,
        )

        # Validate required parameters
        assert self.ssh_host is not None, "SSH host is required"
        assert self.ssh_port is not None, "SSH port is required"
        assert self.ssh_user is not None, "SSH user is required"
        assert self.ssh_pkey is not None, "SSH private key path is required"
        assert self.postgres_port is not None, "Postgres port is required"

        self.ssh_tunnel = modern_ssh_tunnel.SSHTunnelForwarder(
            (self.ssh_host, self.ssh_port),
            ssh_username=self.ssh_user,
            ssh_pkey=self.ssh_pkey,
            remote_bind_address=(self.LOCAL_HOST, self.postgres_port),
        )

        self.ssh_tunnel.start()
        log.print_ok_arrow(f"SSH tunnel active: {self.ssh_tunnel.is_active}")

    def query(self, query: str, verbose: bool = False) -> pd.DataFrame:
        start_time = time.time()

        if verbose:
            log.print_normal("=" * 80)
            log.print_normal(query)
            log.print_normal("=" * 80)

        try:
            df = pd.read_sql_query(query, self.conn)  # type: ignore
            time_delta = time.time() - start_time
            if verbose:
                log.print_ok_arrow(f"Time taken to query database: {time_delta:.2f} seconds")
            return T.cast(pd.DataFrame, df)
        except Exception as e:  # pylint: disable=broad-except
            log.print_normal(f"Error executing query: {e}")

        return pd.DataFrame()

    @abc.abstractmethod
    def clear(self, table: str) -> None:
        pass

    def close(self) -> None:
        # Close connection
        if self.conn is not None:
            try:
                if isinstance(self.conn, SparkSession):
                    self.conn.stop()
                elif isinstance(self.conn, psycopg2.extensions.connection):
                    if not self.conn.closed:
                        self.conn.close()
            except Exception as e:  # pylint: disable=broad-except
                log.print_fail(f"Error closing database connection: {e}")
            finally:
                self.conn = None

        # Close SSH tunnel
        if self.ssh_tunnel is not None:
            try:
                if self.ssh_tunnel.is_active:
                    self.ssh_tunnel.stop()
                    log.print_ok_arrow("SSH tunnel closed successfully.")
            except Exception as e:  # pylint: disable=broad-except
                log.print_fail(f"Error closing SSH tunnel: {e}")
            finally:
                self.ssh_tunnel = None

        log.print_ok_arrow("Connection to the database closed successfully.")

    def copy_db_local(self, local_db_path: str) -> None:
        assert self.db_name is not None, "Database name is required"
        self._maybe_copy_database_locally(local_db_path=local_db_path)
        self._import_local_database(local_db_path=local_db_path, temp_db_name=self.db_name)

    def run_command(self, command: str) -> None:
        """Run a command on the remote server via direct SSH connection."""
        # Validate required parameters
        assert self.ssh_host is not None, "SSH host is required"
        assert self.ssh_port is not None, "SSH port is required"
        assert self.ssh_user is not None, "SSH user is required"
        assert self.ssh_pkey is not None, "SSH private key path is required"

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            client.connect(
                hostname=self.ssh_host,
                port=self.ssh_port,
                username=self.ssh_user,
                key_filename=self.ssh_pkey,
            )

            log.print_normal("Running command on remote server:")
            log.print_normal("=" * 80)
            log.print_normal(command)
            log.print_normal("=" * 80)

            _, stdout, stderr = client.exec_command(command)
            log.print_normal(stdout.read().decode())
            log.print_normal(stderr.read().decode())

        except Exception as e:  # pylint: disable=broad-except
            log.print_fail(f"Failed to run command: {e}")
        finally:
            client.close()


class DbQueryPsycopg2(DbQuery):
    def __init__(self, *args: T.Any, **kwargs: T.Any) -> None:
        super().__init__(*args, **kwargs)
        # Store the actual connection URI for pandas queries
        self._connection_uri: str | None = None

    def connect(self, use_ssh_tunnel: bool = False) -> None:
        postgres_host: str
        bind_port: int

        if use_ssh_tunnel:
            self._establish_ssh_tunnel()
            assert self.ssh_tunnel is not None, "SSH tunnel is not active."
            postgres_host = self.LOCAL_HOST
            bind_port = self.ssh_tunnel.local_bind_port
        else:
            assert self.postgres_host is not None, "Postgres host is required"
            assert self.postgres_port is not None, "Postgres port is required"
            postgres_host = self.postgres_host
            bind_port = self.postgres_port

        # Store the actual connection URI for pandas
        self._connection_uri = (
            f"postgresql://{self.postgres_user}:{self.postgres_password}@"
            f"{postgres_host}:{bind_port}/{self.postgres_database}"
        )

        log.print_normal(f"PostgreSQL bind port: {self.postgres_host} -> {self.postgres_port}")
        # Connect to the PostgreSQL database
        self.conn = psycopg2.connect(
            host=postgres_host,
            port=bind_port,
            dbname=self.postgres_database,
            user=self.postgres_user,
            password=self.postgres_password,
        )
        log.print_ok_arrow("Connection to the database established successfully.")

    def query(self, query: str, verbose: bool = False) -> pd.DataFrame:
        """Override query to use connection URI for pandas."""
        start_time = time.time()

        if verbose:
            log.print_normal("=" * 80)
            log.print_normal(query)
            log.print_normal("=" * 80)

        try:
            # Use the actual connection URI to avoid pandas warning
            connection_uri = self._connection_uri if self._connection_uri else self.postgres_uri
            df = pd.read_sql_query(query, connection_uri)
            time_delta = time.time() - start_time
            if verbose:
                log.print_ok_arrow(f"Time taken to query database: {time_delta:.2f} seconds")
            return T.cast(pd.DataFrame, df)
        except Exception as e:  # pylint: disable=broad-except
            log.print_normal(f"Error executing query: {e}")

        return pd.DataFrame()

    def clear(self, table: str) -> None:
        if self.conn is None:
            log.print_fail("Database connection is not active. Cannot clear table.")
            return

        conn = T.cast(psycopg2.extensions.connection, self.conn)
        clear_query = f"DELETE FROM {table};"
        try:
            with conn.cursor() as cursor:
                cursor.execute(clear_query)
                conn.commit()
                print(f"{table} table cleared successfully.")
        except Exception as e:  # pylint: disable=broad-except
            print(f"Error clearing {table} table: {e}")

    def load_tables(self, tables: T.List[str]) -> T.Dict[str, pd.DataFrame]:
        dfs = {}
        for table in tables:
            table_name, df = self._load_table(table)
            dfs[table_name] = df if df is not None else pd.DataFrame()

        return dfs

    def _load_table(self, table: str) -> T.Tuple[str, pd.DataFrame | None]:
        try:
            query = f"SELECT * FROM {table}"
            return table, self.query(query)
        except Exception as e:  # pylint: disable=broad-except
            print(f"Error loading data for {table}: {e}")
            return table, None


# pylint: disable=too-many-arguments
# pylint: disable=too-many-positional-arguments
class DbQuerySpark(DbQuery):
    JDBC_DRIVER_PATH = "/usr/share/java/postgresql.jar"
    PARALLEL_LOAD = False

    def __init__(
        self,
        postgres_host: str | None = None,
        postgres_port: int | None = None,
        postgres_database: str | None = None,
        postgres_user: str | None = None,
        postgres_password: str | None = None,
        postrgres_url: str | None = None,
        ssh_host: str | None = None,
        ssh_port: int | None = None,
        ssh_user: str | None = None,
        ssh_pkey: str | None = None,
        is_local: bool = False,
        verbose: bool = False,
    ) -> None:
        super().__init__(
            postgres_host,
            postgres_port,
            postgres_database,
            postgres_user,
            postgres_password,
            postrgres_url,
            ssh_host,
            ssh_port,
            ssh_user,
            ssh_pkey,
            is_local,
            verbose,
        )
        self.jdbc_url = (
            f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/{self.db_name}"
        )

        # Validate required parameters for Spark connection
        assert self.postgres_user is not None, "Postgres user is required"
        assert self.postgres_password is not None, "Postgres password is required"

        self.connection_properties: T.Dict[str, str] = {
            "user": self.postgres_user,
            "password": self.postgres_password,
            "driver": "org.postgresql.Driver",
        }

        self.conn = (
            SparkSession.builder.appName("PostgreSQLConnection")
            .config("spark.jars", self.JDBC_DRIVER_PATH)
            .getOrCreate()
        )

    def clear(self, table: str) -> None:
        if self.conn is None:
            log.print_fail("Database connection is not active. Cannot clear table.")
            return
        try:
            self.conn.sql(f"DROP TABLE IF EXISTS {table}")  # type: ignore
            log.print_ok_arrow(f"Table {table} cleared successfully.")
        except Exception as e:  # pylint: disable=broad-except
            log.print_fail(f"Error clearing table {table}: {e}")

    def connect(self, use_ssh_tunnel: bool = False) -> None:
        postgres_host = self.postgres_host
        bind_port = self.postgres_port

        if use_ssh_tunnel:
            self._establish_ssh_tunnel()

            assert self.ssh_tunnel is not None, "SSH tunnel is not active."

            postgres_host = self.LOCAL_HOST
            bind_port = self.ssh_tunnel.local_bind_port

        if self.is_local:
            self.jdbc_url = (
                f"jdbc:postgresql://{self.LOCAL_HOST}:{self.postgres_port}/{self.postgres_database}"
            )
        else:
            self.jdbc_url = f"jdbc:postgresql://{postgres_host}:{bind_port}/{self.db_name}"

        log.print_normal(f"PostgreSQL bind port: {postgres_host} -> {bind_port}")

    def load_tables(self, tables: T.List[str]) -> T.Dict[str, pd.DataFrame]:
        dfs = {}
        if self.PARALLEL_LOAD:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future_to_table = {
                    executor.submit(self._load_table, table): table for table in tables
                }
                for future in concurrent.futures.as_completed(future_to_table):
                    table = future_to_table[future]
                    try:
                        table_name, df = future.result()
                        if df is not None:
                            dfs[table_name] = df
                    except Exception as e:  # pylint: disable=broad-except
                        print(f"Error processing table {table}: {e}")
        else:
            for table in tables:
                table_name, df = self._load_table(table)
                dfs[table_name] = df if df is not None else pd.DataFrame()

        return dfs

    def _load_table(self, table: str) -> T.Tuple[str, pd.DataFrame | None]:
        conn = T.cast(SparkSession, self.conn)
        try:
            spark_df = conn.read.jdbc(
                url=self.jdbc_url, table=self.db(table), properties=self.connection_properties
            )
            return table, spark_df.toPandas()
        except Exception as e:  # pylint: disable=broad-except
            print(f"Error loading data for {table}: {e}")
            return table, None

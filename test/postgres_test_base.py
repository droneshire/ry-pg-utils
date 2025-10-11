"""
Base test class that provides containerized PostgreSQL for testing.

This module provides a base class for tests that require PostgreSQL.
It uses testcontainers to automatically spin up and tear down Docker containers
for testing, eliminating the need for manually running database servers.
"""

import unittest
from typing import Optional

from testcontainers.postgres import PostgresContainer


class PostgresOnlyTestBase(unittest.TestCase):
    """Base test class that provides only PostgreSQL container."""

    postgres_container: Optional[PostgresContainer] = None
    _containers_started = False

    @classmethod
    def setUpClass(cls) -> None:
        """Set up PostgreSQL container once for all tests in the class."""
        super().setUpClass()

        # Start PostgreSQL container
        cls.postgres_container = PostgresContainer("postgres:latest")
        cls.postgres_container.start()

        cls._containers_started = True

    @classmethod
    def tearDownClass(cls) -> None:
        """Tear down PostgreSQL container after all tests in the class."""
        if cls.postgres_container:
            cls.postgres_container.stop()

        cls._containers_started = False
        super().tearDownClass()

    @classmethod
    def get_postgres_connection_params(cls) -> dict:
        """Get PostgreSQL connection parameters.

        Returns:
            dict: Connection parameters for PostgreSQL
        """
        if not cls._containers_started or not cls.postgres_container:
            raise RuntimeError("Container not started. Call setUpClass first.")

        return {
            "host": cls.postgres_container.get_container_host_ip(),
            "port": cls.postgres_container.get_exposed_port(5432),
            "user": cls.postgres_container.username,
            "password": cls.postgres_container.password,
            "dbname": cls.postgres_container.dbname,  # Default database
        }

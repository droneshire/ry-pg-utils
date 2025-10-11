"""
Test PostgresInfo class functionality.

This test verifies that the PostgresInfo class properly handles database
information and null states.
"""

import unittest

from ry_pg_utils.postgres_info import PostgresInfo


class TestPostgresInfo(unittest.TestCase):
    """Test PostgresInfo class functionality."""

    def test_postgres_info_initialization(self) -> None:
        """Test that PostgresInfo can be initialized with valid data."""
        info = PostgresInfo(
            db_name="test_db",
            user="test_user",
            password="test_password",
            host="localhost",
            port=5432,
        )

        self.assertEqual(info.db_name, "test_db")
        self.assertEqual(info.user, "test_user")
        self.assertEqual(info.password, "test_password")
        self.assertEqual(info.host, "localhost")
        self.assertEqual(info.port, 5432)

    def test_postgres_info_null_creation(self) -> None:
        """Test that PostgresInfo.null() creates a proper null object."""
        null_info = PostgresInfo.null()

        self.assertEqual(null_info.db_name, "")
        self.assertEqual(null_info.user, "")
        self.assertEqual(null_info.password, "")
        self.assertEqual(null_info.host, "")
        self.assertEqual(null_info.port, 0)

    def test_postgres_info_is_null(self) -> None:
        """Test the is_null() method with various states."""
        # Test null object
        null_info = PostgresInfo.null()
        self.assertTrue(null_info.is_null())

        # Test valid object
        valid_info = PostgresInfo(
            db_name="test_db",
            user="test_user",
            password="test_password",
            host="localhost",
            port=5432,
        )
        self.assertFalse(valid_info.is_null())

        # Test object with empty database name
        empty_db_info = PostgresInfo(
            db_name="", user="test_user", password="test_password", host="localhost", port=5432
        )
        self.assertTrue(empty_db_info.is_null())

        # Test object with whitespace database name
        whitespace_db_info = PostgresInfo(
            db_name="   ", user="test_user", password="test_password", host="localhost", port=5432
        )
        self.assertTrue(whitespace_db_info.is_null())

        # Test object with empty host
        empty_host_info = PostgresInfo(
            db_name="test_db", user="test_user", password="test_password", host="", port=5432
        )
        self.assertTrue(empty_host_info.is_null())

        # Test object with empty user
        empty_user_info = PostgresInfo(
            db_name="test_db", user="", password="test_password", host="localhost", port=5432
        )
        self.assertTrue(empty_user_info.is_null())

        # Test object with port 0
        zero_port_info = PostgresInfo(
            db_name="test_db", user="test_user", password="test_password", host="localhost", port=0
        )
        self.assertTrue(zero_port_info.is_null())

    def test_postgres_info_equality(self) -> None:
        """Test equality comparison between PostgresInfo objects."""
        info1 = PostgresInfo(
            db_name="test_db",
            user="test_user",
            password="test_password",
            host="localhost",
            port=5432,
        )

        info2 = PostgresInfo(
            db_name="test_db",
            user="test_user",
            password="test_password",
            host="localhost",
            port=5432,
        )

        info3 = PostgresInfo(
            db_name="different_db",
            user="test_user",
            password="test_password",
            host="localhost",
            port=5432,
        )

        # Test equality
        self.assertEqual(info1, info2)
        self.assertNotEqual(info1, info3)

        # Test null equality
        null1 = PostgresInfo.null()
        null2 = PostgresInfo.null()
        self.assertEqual(null1, null2)

        # Test comparison with non-PostgresInfo object
        self.assertNotEqual(info1, "not a PostgresInfo object")
        self.assertNotEqual(info1, None)

    def test_postgres_info_string_representation(self) -> None:
        """Test the string representation of PostgresInfo objects."""
        info = PostgresInfo(
            db_name="test_db",
            user="test_user",
            password="secret_password",
            host="localhost",
            port=5432,
        )

        str_repr = str(info)

        # Should contain all fields
        self.assertIn("test_db", str_repr)
        self.assertIn("test_user", str_repr)
        self.assertIn("localhost", str_repr)
        self.assertIn("5432", str_repr)

        # Password should be masked with exactly 8 asterisks
        self.assertIn("********", str_repr)
        self.assertNotIn("secret_password", str_repr)

        # Should have proper formatting
        self.assertIn("PostgresInfo(", str_repr)
        self.assertIn("\n\t", str_repr)

    def test_postgres_info_null_string_representation(self) -> None:
        """Test string representation of null PostgresInfo object."""
        null_info = PostgresInfo.null()
        str_repr = str(null_info)

        # Should contain empty values
        self.assertIn("db_name=", str_repr)
        self.assertIn("user=", str_repr)
        self.assertIn("host=", str_repr)
        self.assertIn("port=0", str_repr)

        # Should have proper formatting
        self.assertIn("PostgresInfo(", str_repr)

    def test_postgres_info_edge_cases(self) -> None:
        """Test edge cases for PostgresInfo."""
        # Test with very long values
        long_db_name = "a" * 1000
        long_user = "b" * 1000
        long_password = "c" * 1000
        long_host = "d" * 1000

        info = PostgresInfo(
            db_name=long_db_name, user=long_user, password=long_password, host=long_host, port=65535
        )

        self.assertEqual(info.db_name, long_db_name)
        self.assertEqual(info.user, long_user)
        self.assertEqual(info.password, long_password)
        self.assertEqual(info.host, long_host)
        self.assertEqual(info.port, 65535)
        self.assertFalse(info.is_null())

        # Test with special characters
        special_info = PostgresInfo(
            db_name="test-db_123",
            user="user@domain.com",
            password="p@ssw0rd!",
            host="192.168.1.1",
            port=5432,
        )

        self.assertEqual(special_info.db_name, "test-db_123")
        self.assertEqual(special_info.user, "user@domain.com")
        self.assertEqual(special_info.password, "p@ssw0rd!")
        self.assertEqual(special_info.host, "192.168.1.1")
        self.assertFalse(special_info.is_null())

    def test_postgres_info_whitespace_handling(self) -> None:
        """Test that whitespace is properly handled in is_null()."""
        # Test with leading/trailing whitespace
        whitespace_info = PostgresInfo(
            db_name="  test_db  ",
            user="  test_user  ",
            password="test_password",
            host="  localhost  ",
            port=5432,
        )

        # Should not be null because the values are not empty after stripping
        self.assertFalse(whitespace_info.is_null())

        # Test with only whitespace
        only_whitespace_info = PostgresInfo(
            db_name="   ", user="   ", password="test_password", host="   ", port=5432
        )

        # Should be null because the values are empty after stripping
        self.assertTrue(only_whitespace_info.is_null())


if __name__ == "__main__":
    unittest.main()

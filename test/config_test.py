#!/usr/bin/env python3
"""
Tests for the config system, including runtime override functionality.
"""

import os
import unittest
from unittest.mock import patch

from ry_pg_utils.config import (
    ConfigManager,
    get_config,
    has_config_overrides,
    pg_config,
    reset_config,
    set_config,
)


class TestConfigManager(unittest.TestCase):
    """Test the ConfigManager singleton and its methods."""

    def setUp(self) -> None:
        """Reset config before each test."""
        reset_config()

    def test_singleton_pattern(self) -> None:
        """Test that ConfigManager follows singleton pattern."""
        manager1 = ConfigManager()
        manager2 = ConfigManager()
        self.assertIs(manager1, manager2)

    def test_initial_config_loads_from_env(self) -> None:
        """Test that initial config loads from environment variables."""
        with patch.dict(os.environ, {"POSTGRES_HOST": "env-host", "POSTGRES_PORT": "5432"}):
            reset_config()  # Force reload
            config = get_config()
            self.assertEqual(config.postgres_host, "env-host")
            self.assertEqual(config.postgres_port, 5432)

    def test_set_config_single_value(self) -> None:
        """Test setting a single config value."""
        set_config(postgres_host="test-host")

        config = get_config()
        self.assertTrue(has_config_overrides())
        self.assertEqual(config.postgres_host, "test-host")

    def test_set_config_multiple_values(self) -> None:
        """Test setting multiple config values."""
        set_config(
            postgres_host="test-host",
            postgres_port=9999,
            postgres_db="test-db",
        )

        config = get_config()
        self.assertTrue(has_config_overrides())
        self.assertEqual(config.postgres_host, "test-host")
        self.assertEqual(config.postgres_port, 9999)
        self.assertEqual(config.postgres_db, "test-db")

    def test_set_config_preserves_other_values(self) -> None:
        """Test that set_config only changes specified values."""
        # Get initial config
        initial_config = get_config()
        original_do_publish = initial_config.do_publish_db

        # Set only one value
        set_config(postgres_host="new-host")

        config = get_config()
        self.assertEqual(config.postgres_host, "new-host")
        self.assertEqual(config.do_publish_db, original_do_publish)  # Should be unchanged

    def test_config_persistence_across_calls(self) -> None:
        """Test that config overrides persist across multiple get_config() calls."""
        set_config(postgres_host="persistent-host")

        # Multiple calls should return the same overridden value
        config1 = get_config()
        config2 = get_config()

        self.assertEqual(config1.postgres_host, "persistent-host")
        self.assertEqual(config2.postgres_host, "persistent-host")
        self.assertTrue(has_config_overrides())

    def test_reset_config_clears_overrides(self) -> None:
        """Test that reset_config clears overrides and reloads from env."""
        set_config(postgres_host="test-host")
        self.assertTrue(has_config_overrides())

        reset_config()
        self.assertFalse(has_config_overrides())

        # Should reload from environment/defaults
        config = get_config()
        self.assertNotEqual(config.postgres_host, "test-host")

    def test_has_config_overrides_tracking(self) -> None:
        """Test that has_config_overrides correctly tracks override state."""
        # Initially no overrides
        self.assertFalse(has_config_overrides())

        # After setting config, should have overrides
        set_config(postgres_host="test-host")
        self.assertTrue(has_config_overrides())

        # After reset, should have no overrides
        reset_config()
        self.assertFalse(has_config_overrides())

    def test_set_config_before_first_get(self) -> None:
        """Test that set_config works even before first get_config() call."""
        # Set config before any get_config() call
        set_config(postgres_host="early-set-host")

        # First get_config() should return the overridden value
        config = get_config()
        self.assertEqual(config.postgres_host, "early-set-host")
        self.assertTrue(has_config_overrides())

    def test_multiple_set_config_calls(self) -> None:
        """Test that multiple set_config calls work correctly."""
        set_config(postgres_host="first-host")
        set_config(postgres_port=1111)
        set_config(postgres_db="final-db")

        config = get_config()
        self.assertEqual(config.postgres_host, "first-host")
        self.assertEqual(config.postgres_port, 1111)
        self.assertEqual(config.postgres_db, "final-db")
        self.assertTrue(has_config_overrides())

    def test_config_dataclass_mutability(self) -> None:
        """Test that Config objects can be modified directly (dataclasses are mutable)."""
        config = get_config()
        original_host = config.postgres_host

        # Dataclasses are mutable by default, so this should work
        config.postgres_host = "modified-host"
        self.assertEqual(config.postgres_host, "modified-host")

        # Since get_config() returns the same instance, this affects the global config
        global_config = get_config()
        self.assertEqual(global_config.postgres_host, "modified-host")

        # Reset to original state
        config.postgres_host = original_host


class TestConfigIntegration(unittest.TestCase):
    """Test config integration with other modules."""

    def setUp(self) -> None:
        """Reset config before each test."""
        reset_config()

    def test_import_order_independence(self) -> None:
        """Test that config overrides work regardless of import order."""
        # Simulate importing config before setting overrides
        initial_config = get_config()
        _ = initial_config.postgres_host

        # Apply overrides
        set_config(postgres_host="override-host")

        # Get config again - should have the override
        updated_config = get_config()
        self.assertEqual(updated_config.postgres_host, "override-host")
        self.assertTrue(has_config_overrides())

    def test_backward_compatibility_pg_config(self) -> None:
        """Test that pg_config() function still works for backward compatibility."""
        set_config(postgres_host="compat-host")

        # Both should return the same config
        config1 = get_config()
        config2 = pg_config()

        self.assertEqual(config1.postgres_host, "compat-host")
        self.assertEqual(config2.postgres_host, "compat-host")
        self.assertIs(config1, config2)

    def test_config_with_environment_variables(self) -> None:
        """Test config behavior with various environment variable scenarios."""
        with patch.dict(
            os.environ,
            {
                "POSTGRES_HOST": "env-host",
                "POSTGRES_PORT": "5432",
                "POSTGRES_DB": "env-db",
                "POSTGRES_USER": "env-user",
            },
            clear=True,
        ):
            reset_config()  # Force reload from env

            config = get_config()
            self.assertEqual(config.postgres_host, "env-host")
            self.assertEqual(config.postgres_port, 5432)
            self.assertEqual(config.postgres_db, "env-db")
            self.assertEqual(config.postgres_user, "env-user")
            self.assertFalse(has_config_overrides())

    def test_config_with_mixed_env_and_overrides(self) -> None:
        """Test config with both environment variables and runtime overrides."""
        with patch.dict(
            os.environ,
            {
                "POSTGRES_HOST": "env-host",
                "POSTGRES_PORT": "5432",
            },
            clear=True,
        ):
            reset_config()  # Force reload from env

            # Override some values
            set_config(postgres_host="override-host", postgres_db="override-db")

            config = get_config()
            self.assertEqual(config.postgres_host, "override-host")  # Overridden
            self.assertEqual(config.postgres_port, 5432)  # From env
            self.assertEqual(config.postgres_db, "override-db")  # Overridden
            self.assertTrue(has_config_overrides())

    def test_config_default_values(self) -> None:
        """Test that config has appropriate default values."""
        with patch.dict(os.environ, {}, clear=True):
            reset_config()  # Force reload with empty env

            config = get_config()
            # Check some default values
            self.assertTrue(config.do_publish_db)
            self.assertTrue(config.use_local_db_only)
            self.assertTrue(config.raise_on_use_before_init)


class TestConfigEdgeCases(unittest.TestCase):
    """Test edge cases and error conditions."""

    def setUp(self) -> None:
        """Reset config before each test."""
        reset_config()

    def test_set_config_with_none_values(self) -> None:
        """Test setting config values to None."""
        set_config(postgres_host=None, postgres_port=None)

        config = get_config()
        self.assertIsNone(config.postgres_host)
        self.assertIsNone(config.postgres_port)
        self.assertTrue(has_config_overrides())

    def test_set_config_with_invalid_field(self) -> None:
        """Test that set_config raises error for invalid fields."""
        with self.assertRaises(TypeError):
            set_config(invalid_field="should-fail")

    def test_set_config_empty(self) -> None:
        """Test calling set_config with no arguments."""
        initial_config = get_config()
        set_config()  # No arguments

        config = get_config()
        self.assertEqual(config, initial_config)  # Should be unchanged
        self.assertFalse(has_config_overrides())

    def test_reset_config_multiple_times(self) -> None:
        """Test calling reset_config multiple times."""
        set_config(postgres_host="test-host")
        self.assertTrue(has_config_overrides())

        reset_config()
        self.assertFalse(has_config_overrides())

        reset_config()  # Second reset
        self.assertFalse(has_config_overrides())

        config = get_config()
        self.assertNotEqual(config.postgres_host, "test-host")

    def test_config_thread_safety_simulation(self) -> None:
        """Test that config operations are thread-safe (simulated)."""
        # This is a basic test - in a real multithreaded environment,
        # you'd want more comprehensive testing

        set_config(postgres_host="thread-test-host")

        # Simulate multiple threads accessing config
        configs = []
        for _ in range(10):
            configs.append(get_config())

        # All should be the same
        for config in configs:
            self.assertEqual(config.postgres_host, "thread-test-host")
            self.assertTrue(has_config_overrides())


if __name__ == "__main__":
    unittest.main()

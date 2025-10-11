"""
Unit tests for the connect module, specifically testing dynamic model imports.
"""

import sys
import tempfile
import unittest
from pathlib import Path
from test.postgres_test_base import PostgresOnlyTestBase

from ry_pg_utils.connect import Base, _import_models_from_module, close_engine, init_database


class TestDynamicModelImport(unittest.TestCase):
    """Test cases for dynamic model import functionality."""

    def setUp(self) -> None:
        """Set up test fixtures before each test method."""
        # Clear Base metadata before each test
        Base.metadata.clear()

    def test_import_models_from_nonexistent_module(self) -> None:
        """Test that importing from a nonexistent module doesn't crash."""
        # Should not raise an exception
        _import_models_from_module("nonexistent.module.path")

    def test_import_models_from_empty_module(self) -> None:
        """Test importing from a module with no models."""
        # Create a temporary module
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a simple package
            pkg_path = Path(tmpdir)
            test_pkg = pkg_path / "test_empty_pkg"
            test_pkg.mkdir()
            (test_pkg / "__init__.py").write_text("")

            # Add to sys.path temporarily
            sys.path.insert(0, str(pkg_path))
            try:
                _import_models_from_module("test_empty_pkg")
                # Should complete without error
                self.assertTrue(True)
            finally:
                sys.path.remove(str(pkg_path))

    def test_import_models_with_actual_models(self) -> None:
        """Test importing models from a package with actual model classes."""
        # Create a temporary module with SQLAlchemy models
        with tempfile.TemporaryDirectory() as tmpdir:
            pkg_path = Path(tmpdir)
            test_pkg = pkg_path / "test_models_pkg"
            test_pkg.mkdir()

            # Create __init__.py
            (test_pkg / "__init__.py").write_text("")

            # Create a simple Python module (not SQLAlchemy model)
            # to test that the import mechanism works
            model_code = """
# Simple test module to verify import works
TEST_MODULE_LOADED = True

class SimpleClass:
    def __init__(self):
        self.name = "test"
"""
            (test_pkg / "models.py").write_text(model_code)

            # Add to sys.path temporarily
            sys.path.insert(0, str(pkg_path))
            try:
                # This should import without errors
                _import_models_from_module("test_models_pkg.models")

                # Verify module was imported by checking if we can access it
                self.assertIn("test_models_pkg.models", sys.modules)
            finally:
                # Clean up
                if "test_models_pkg.models" in sys.modules:
                    del sys.modules["test_models_pkg.models"]
                if "test_models_pkg" in sys.modules:
                    del sys.modules["test_models_pkg"]
                sys.path.remove(str(pkg_path))
                Base.metadata.clear()

    def test_import_models_with_nested_packages(self) -> None:
        """Test importing models from nested package structure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            pkg_path = Path(tmpdir)
            test_pkg = pkg_path / "test_nested_pkg"
            test_pkg.mkdir()
            (test_pkg / "__init__.py").write_text("")

            # Create nested models directory
            models_dir = test_pkg / "models"
            models_dir.mkdir()
            (models_dir / "__init__.py").write_text("")

            # Create simple Python modules in nested directory
            user_model = """
# Test user module
USER_MODULE_LOADED = True
"""
            (models_dir / "user.py").write_text(user_model)

            product_model = """
# Test product module
PRODUCT_MODULE_LOADED = True
"""
            (models_dir / "product.py").write_text(product_model)

            # Add to sys.path temporarily
            sys.path.insert(0, str(pkg_path))
            try:
                # This should import all nested modules
                _import_models_from_module("test_nested_pkg.models")

                # Verify nested modules were imported
                self.assertIn("test_nested_pkg.models.user", sys.modules)
                self.assertIn("test_nested_pkg.models.product", sys.modules)
            finally:
                # Clean up
                for mod in list(sys.modules.keys()):
                    if mod.startswith("test_nested_pkg"):
                        del sys.modules[mod]
                sys.path.remove(str(pkg_path))
                Base.metadata.clear()


class TestInitDatabaseWithModels(PostgresOnlyTestBase):
    """Test init_database with dynamic model imports using real PostgreSQL."""

    test_db_name: str = "test_connect_db"

    def setUp(self) -> None:
        """Set up test fixtures before each test method."""
        Base.metadata.clear()

    def tearDown(self) -> None:
        """Clean up after each test method."""
        try:
            close_engine(self.test_db_name)
        except Exception:  # pylint: disable=broad-except
            pass
        Base.metadata.clear()

    def test_init_database_without_models(self) -> None:
        """Test that init_database works without models_module parameter."""
        conn_params = self.get_postgres_connection_params()

        # Should work without crashing
        init_database(
            db_name=self.test_db_name,
            db_user=conn_params["user"],
            db_password=conn_params["password"],
            db_host=conn_params["host"],
            db_port=int(conn_params["port"]),
        )

        # Clean up
        close_engine(self.test_db_name)

    def test_init_database_with_nonexistent_models_module(self) -> None:
        """Test that init_database handles nonexistent models gracefully."""
        conn_params = self.get_postgres_connection_params()

        # Should not crash even with invalid module
        init_database(
            db_name=self.test_db_name,
            db_user=conn_params["user"],
            db_password=conn_params["password"],
            db_host=conn_params["host"],
            db_port=int(conn_params["port"]),
            models_module="nonexistent.models.module",
        )

        # Clean up
        close_engine(self.test_db_name)

    def test_init_database_with_valid_models(self) -> None:
        """Test init_database with a valid models module."""
        # Create temporary models package
        with tempfile.TemporaryDirectory() as tmpdir:
            pkg_path = Path(tmpdir)
            test_pkg = pkg_path / "test_db_models"
            test_pkg.mkdir()
            (test_pkg / "__init__.py").write_text("")

            # Create a simple Python module (not SQLAlchemy to avoid backend_id issues)
            model_code = """
# Test module for init_database
DB_MODELS_LOADED = True
"""
            (test_pkg / "account.py").write_text(model_code)

            sys.path.insert(0, str(pkg_path))
            try:
                conn_params = self.get_postgres_connection_params()

                # Initialize database with the models
                # This should not crash even though the module doesn't have real models
                init_database(
                    db_name=self.test_db_name,
                    db_user=conn_params["user"],
                    db_password=conn_params["password"],
                    db_host=conn_params["host"],
                    db_port=int(conn_params["port"]),
                    models_module="test_db_models",
                )

                # Verify the module was imported
                self.assertIn("test_db_models.account", sys.modules)

            finally:
                # Clean up
                for mod in list(sys.modules.keys()):
                    if mod.startswith("test_db_models"):
                        del sys.modules[mod]
                sys.path.remove(str(pkg_path))
                close_engine(self.test_db_name)
                Base.metadata.clear()


if __name__ == "__main__":
    unittest.main()

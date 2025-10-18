import os
import threading
from dataclasses import dataclass, replace
from typing import Any, Optional

import dotenv


@dataclass
class Config:
    postgres_host: str | None
    postgres_port: int | None
    postgres_db: str | None
    postgres_user: str | None
    postgres_password: str | None
    do_publish_db: bool
    use_local_db_only: bool
    raise_on_use_before_init: bool
    ssh_host: str | None
    ssh_port: int | None
    ssh_user: str | None
    ssh_key_path: str | None


class ConfigManager:
    """Thread-safe lazy-loaded configuration manager with runtime override support."""

    _instance: Optional["ConfigManager"] = None
    _lock = threading.Lock()
    _config: Optional[Config] = None
    _dotenv_loaded: bool = False
    _overrides_applied: bool = False

    def __new__(cls) -> "ConfigManager":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def get_config(self) -> Config:
        """Get the configuration, loading it lazily if needed."""
        if self._config is None:
            with self._lock:
                if self._config is None:
                    self._config = self._load_config()
        return self._config

    def _load_config(self) -> Config:
        """Load configuration from environment variables."""
        # Only load dotenv if not already loaded
        if not self._dotenv_loaded:
            dotenv.load_dotenv()
            self._dotenv_loaded = True

        # Parse POSTGRES_PORT with proper None handling for mypy
        _postgres_port_str = os.getenv("POSTGRES_PORT")
        _postgres_port = int(_postgres_port_str) if _postgres_port_str is not None else None

        _ssh_port_str = os.getenv("SSH_PORT")
        _ssh_port = int(_ssh_port_str) if _ssh_port_str is not None else None

        return Config(
            postgres_host=os.getenv("POSTGRES_HOST"),
            postgres_port=_postgres_port,
            postgres_db=os.getenv("POSTGRES_DB"),
            postgres_user=os.getenv("POSTGRES_USER"),
            postgres_password=os.getenv("POSTGRES_PASSWORD"),
            do_publish_db=True,
            use_local_db_only=True,
            raise_on_use_before_init=True,
            ssh_host=os.getenv("SSH_HOST"),
            ssh_port=_ssh_port,
            ssh_user=os.getenv("SSH_USER"),
            ssh_key_path=os.getenv("SSH_KEY_PATH"),
        )

    def set_config(self, **kwargs: Any) -> None:
        """Set configuration values at runtime. Thread-safe and respects import order."""
        with self._lock:
            if self._config is None:
                # Load config first if not already loaded
                self._config = self._load_config()

            # Only create new config and set overrides flag if kwargs are provided
            if kwargs:
                # Create new config with overridden values
                self._config = replace(self._config, **kwargs)
                self._overrides_applied = True

    def has_overrides(self) -> bool:
        """Check if runtime overrides have been applied."""
        return self._overrides_applied

    def reset_config(self) -> None:
        """Reset the configuration (useful for testing)."""
        with self._lock:
            self._config = None
            self._dotenv_loaded = False
            self._overrides_applied = False


# Global config manager instance
_config_manager = ConfigManager()


def get_config() -> Config:
    """Get the current configuration."""
    return _config_manager.get_config()


def set_config(**kwargs: Any) -> None:
    """Set configuration values at runtime. Thread-safe and respects import order.

    Example:
        set_config(postgres_host="localhost", postgres_port=5432)
    """
    _config_manager.set_config(**kwargs)


def has_config_overrides() -> bool:
    """Check if runtime overrides have been applied."""
    return _config_manager.has_overrides()


def reset_config() -> None:
    """Reset the configuration (useful for testing)."""
    _config_manager.reset_config()


# Backward compatibility - but now lazy-loaded
def pg_config() -> Config:
    """Backward compatibility function for pg_config."""
    return get_config()

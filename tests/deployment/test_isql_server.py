"""Tests for the internal ISQL service connection behavior."""

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import sys
from types import ModuleType
from unittest.mock import Mock

import pytest

_ISQL_SOURCE_DIR = Path(__file__).parents[2] / "docker" / "isql-service" / "src"


def test_connections_enable_autocommit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Successful mutation statements must persist when the connection closes."""
    connection = Mock()
    fake_pyodbc = ModuleType("pyodbc")
    fake_pyodbc.Connection = object  # type: ignore[attr-defined]
    fake_pyodbc.connect = Mock(return_value=connection)  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "pyodbc", fake_pyodbc)
    monkeypatch.syspath_prepend(str(_ISQL_SOURCE_DIR))
    monkeypatch.setenv("ISQL_SERVICE_TOKEN", "test-token")
    monkeypatch.chdir(tmp_path)

    spec = spec_from_file_location(
        "isql_service_server_under_test", _ISQL_SOURCE_DIR / "server.py"
    )
    assert spec is not None and spec.loader is not None
    server = module_from_spec(spec)
    spec.loader.exec_module(server)

    connection_manager_class = getattr(server, "ConnectionManager")  # noqa: B009
    manager = connection_manager_class("DRIVER=test", timeout=42)
    assert manager.get_connection() is connection
    fake_pyodbc.connect.assert_called_once_with(  # type: ignore[attr-defined]
        "DRIVER=test",
        timeout=42,
        autocommit=True,
    )

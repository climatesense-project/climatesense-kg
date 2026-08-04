"""Tests for the internal ISQL service connection behavior."""

import ast
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import sys
from types import ModuleType
from unittest.mock import Mock

import pytest

_ISQL_SOURCE_DIR = Path(__file__).parents[2] / "docker" / "isql-service" / "src"


def test_odbc_routes_are_synchronous_for_fastapi_threadpool() -> None:
    """Blocking ODBC work must run as synchronous FastAPI route functions."""
    tree = ast.parse((_ISQL_SOURCE_DIR / "server.py").read_text(encoding="utf-8"))
    route_functions = {
        node.name: node
        for node in tree.body
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
        and node.name in {"health_check", "execute_sql_query"}
    }

    assert set(route_functions) == {"health_check", "execute_sql_query"}
    assert all(isinstance(node, ast.FunctionDef) for node in route_functions.values())


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
    monkeypatch.setenv("VIRTUOSO_PASSWORD", "test-password")
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


def test_query_failure_closes_cursor_and_connection(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Exceptional query paths must release both ODBC resources."""
    fake_pyodbc = ModuleType("pyodbc")
    fake_pyodbc.Connection = object  # type: ignore[attr-defined]
    fake_pyodbc.connect = Mock()  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "pyodbc", fake_pyodbc)
    monkeypatch.syspath_prepend(str(_ISQL_SOURCE_DIR))
    monkeypatch.setenv("ISQL_SERVICE_TOKEN", "test-token")
    monkeypatch.setenv("VIRTUOSO_PASSWORD", "test-password")
    monkeypatch.chdir(tmp_path)

    spec = spec_from_file_location(
        "isql_service_server_cleanup_test", _ISQL_SOURCE_DIR / "server.py"
    )
    assert spec is not None and spec.loader is not None
    server = module_from_spec(spec)
    spec.loader.exec_module(server)

    cursor = Mock()
    cursor.execute.side_effect = RuntimeError("query failed")
    connection = Mock()
    connection.cursor.return_value = cursor
    server.connection_manager.get_connection = Mock(return_value=connection)
    request = Mock(headers={"Authorization": "Bearer test-token"})
    query = server.QueryRequest(query="SELECT broken")

    with pytest.raises(server.HTTPException) as exc_info:
        server.execute_sql_query(query, request)

    assert exc_info.value.status_code == 400
    cursor.close.assert_called_once_with()
    connection.close.assert_called_once_with()

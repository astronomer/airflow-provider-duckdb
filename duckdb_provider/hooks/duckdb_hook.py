from __future__ import annotations
from typing import Dict

import duckdb

from airflow.exceptions import AirflowException
from airflow.providers.common.sql.hooks.sql import DbApiHook


class DuckDBHook(DbApiHook):
    """Interact with DuckDB."""

    conn_name_attr = "duckdb_conn_id"
    default_conn_name = "duckdb_default"
    conn_type = "duckdb"
    hook_name = "DuckDB"
    placeholder = "?"

    def __init__(self, *args, **kwargs):
        "Validates the connection arguments"
        super().__init__(*args, **kwargs)

        if self.is_motherduck and self.is_local:
            raise AirflowException(
                "You cannot set both host and schema! If you're connecting to a local file, set host to the path to the file. If you're connecting to a MotherDuck instance, set schema to the database name."
            )

    def get_conn(self) -> duckdb.DuckDBPyConnection:
        """Returns a duckdb connection object"""
        conn_id = getattr(self, self.conn_name_attr)
        airflow_conn = self.get_connection(conn_id)

        if self.is_motherduck:
            db_name = airflow_conn.schema or ""
            return duckdb.connect(
                f"md:{db_name}?motherduck_token={airflow_conn.password}"
            )

        if self.is_local:
            return duckdb.connect(airflow_conn.host)

        return duckdb.connect(":memory:")

    def get_uri(self) -> str:
        """Override DbApiHook get_uri method for get_sqlalchemy_engine()"""
        conn_id = getattr(self, self.conn_name_attr)
        airflow_conn = self.get_connection(conn_id)

        if self.is_motherduck:
            db_name = airflow_conn.schema or ""
            return f"duckdb:///md:{db_name}?motherduck_token={airflow_conn.password}"

        if self.is_local:
            return f"duckdb:///{airflow_conn.host}"

        return "duckdb:///:memory:"

    @property
    def is_motherduck(self) -> bool:
        "Returns True if the connection is to a MotherDuck instance"
        conn_id = getattr(self, self.conn_name_attr)
        airflow_conn = self.get_connection(conn_id)

        return bool(airflow_conn.password)

    @property
    def is_local(self) -> bool:
        "Returns True if the connection is to a local file"
        conn_id = getattr(self, self.conn_name_attr)
        airflow_conn = self.get_connection(conn_id)

        return bool(airflow_conn.host)

    @staticmethod
    def get_ui_field_behaviour() -> Dict:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ["login", "port", "extra"],
            "relabeling": {
                "host": "Path to local database file",
                "schema": "MotherDuck database name",
                "password": "MotherDuck Service token",
            },
        }

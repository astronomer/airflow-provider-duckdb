from __future__ import annotations
from typing import Dict

import duckdb

from airflow.providers.common.sql.hooks.sql import DbApiHook


class DuckDBHook(DbApiHook):
    """Interact with DuckDB."""

    conn_name_attr = "duckdb_conn_id"
    default_conn_name = "duckdb_default"
    conn_type = "duckdb"
    hook_name = "DuckDB"
    placeholder = "?"

    def get_conn(self) -> duckdb.DuckDBPyConnection:
        """Returns a duckdb connection object"""
        # get the conn_id from the hook
        conn_id = getattr(self, self.conn_name_attr)

        # get the airflow connection object with config
        airflow_conn = self.get_connection(conn_id)

        # if no host was specified use an in-memory database (empty string)
        if not airflow_conn.host:
            db_name = ""
        else:
            db_name = airflow_conn.host

        # return the connection to MotherDuck if a token was specified
        # if db_name is empty, the default MotherDuck database will be used
        if airflow_conn.password:
            return duckdb.connect("motherduck:" + db_name "?token=" + airflow_conn.password)
        
        # otherwise return a local connection
        return duckdb.connect(db_name)

    def get_uri(self) -> str:
        """Override DbApiHook get_uri method for get_sqlalchemy_engine()"""
        # get the conn_id from the hook
        conn_id = getattr(self, self.conn_name_attr)

        # get the airflow connection object with config
        airflow_conn = self.get_connection(conn_id)

        # if a token was given, return a MotherDuck URI
        if airflow_conn.password:
            return "motherduck:" + airflow_conn.host + "?token=" + airflow_conn.password

        # if we don't have a host, assume we're using an in-memory database
        if not airflow_conn.host:
            return "duckdb:///:memory:"

        # otherwise return the host
        return f"duckdb:///{airflow_conn.host}"

    @staticmethod
    def get_ui_field_behaviour() -> Dict:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ["login", "schema", "port", "extra"],
            "relabeling": {
                "host": "File or MotherDuck database (leave blank for in-memory database)",
                "password": "MotherDuck Service token (leave blank for local database)"
            },
        }

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

        # define the connection string based on the specified service
        if airflow_conn.host == "motherduck":
            if not airflow_conn.password:
                raise ValueError("Connecting to MotherDuck requires a token!")
            else:
                connection_string = f"motherduck:{airflow_conn.schema}?token={airflow_conn.password}"
        elif airflow_conn.host == "duckdb":
            connection_string = f"{airflow_conn.schema}"
        else:
            raise ValueError("Service name must be 'motherduck' or 'duckdb'")
        
        print(f"Connecting to: {connection_string} ...")

        return duckdb.connect(connection_string)

    def get_uri(self) -> str:
        """Override DbApiHook get_uri method for get_sqlalchemy_engine()"""

        # get the conn_id from the hook
        conn_id = getattr(self, self.conn_name_attr)

        # get the airflow connection object with config
        airflow_conn = self.get_connection(conn_id)

        # define the connection string based on the specified service
        if airflow_conn.host == "motherduck":
            if not airflow_conn.password:
                raise ValueError("Connecting to MotherDuck requires a token!")
            else:
                connection_string = f"motherduck:{airflow_conn.schema}?motherduck_token={airflow_conn.password}"
        elif airflow_conn.host == "duckdb":
            connection_string = f"{airflow_conn.schema}"
        else:
            raise ValueError("Service name must be 'motherduck' or 'duckdb'")
        
        print(f"Connecting to: {connection_string} ...")

        # return the URI for SQLAlchemy
        return f"duckdb:///{connection_string}"


    @staticmethod
    def get_ui_field_behaviour() -> Dict:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ["login", "port", "extra"],
            "relabeling": {
                "host": "Service name ('motherduck' or 'duckdb')",
                "schema": "Filepath to a local DuckDB database or name of a MotherDuck table (leave blank for in-memory database)",
                "password": "MotherDuck Service token (leave blank for local database)"
            },
        }

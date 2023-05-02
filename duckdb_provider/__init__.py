def get_provider_info():
    return {
        "package-name": "airflow-provider-duckdb",
        "name": "DuckDB Airflow Provider",
        "description": "DuckDB (duckdb.org) provider for Apache Airflow",
        "hook-class-names": ["duckdb_provider.hooks.duckdb_hook.DuckDBHook"],
        "connection-types": [
            {"connection-type": "duckdb", "hook-class-name": "duckdb_provider.hooks.duckdb_hook.DuckDBHook"}
        ],
        "versions": ["0.1.0"],
    }

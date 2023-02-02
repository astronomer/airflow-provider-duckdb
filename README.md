# airflow-provider-duckdb

A DuckDB provider for Airflow. This provider exposes a hook/connection that returns a DuckDB connection.

## Installation

```bash
pip install airflow-provider-duckdb
```

## Connection

The connection type is `duckdb`. It supports setting the following parameters:

- `file` (optional): The path to the DuckDB database file. If not set, operations will be done in-memory.

Example connection strings:

- `duckdb://:memory:`
- `duckdb:///tmp/duckdb.db`

## Usage

```python
import pandas as pd
import pendulum

from airflow.decorators import dag, task
from duckdb_provider.hooks.duckdb_hook import DuckDBHook


@dag(
    schedule=None,
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
)
def duckdb_transform():
    @task
    def create_df() -> pd.DataFrame:
        """
        Create a dataframe with some sample data
        """
        df = pd.DataFrame(
            {
                "a": [1, 2, 3],
                "b": [4, 5, 6],
                "c": [7, 8, 9],
            }
        )
        return df

    @task
    def simple_select(df: pd.DataFrame) -> pd.DataFrame:
        """
        Use DuckDB to select a subset of the data
        """
        hook = DuckDBHook.get_hook('duckdb_default')
        conn = hook.get_conn()

        # execute a simple query
        res = conn.execute("SELECT a, b, c FROM df WHERE a >= 2").df()

        return res

    @task
    def add_col(df: pd.DataFrame) -> pd.DataFrame:
        """
        Use DuckDB to add a column to the data
        """
        hook = DuckDBHook.get_hook('duckdb_default')
        conn = hook.get_conn()

        # add a column
        conn.execute("CREATE TABLE tb AS SELECT *, a + b AS d FROM df")

        # get the table
        return conn.execute("SELECT * FROM tb").df()

    @task
    def aggregate(df: pd.DataFrame) -> pd.DataFrame:
        """
        Use DuckDB to aggregate the data
        """
        hook = DuckDBHook.get_hook('duckdb_default')
        conn = hook.get_conn()

        # aggregate
        return conn.execute("SELECT SUM(a), COUNT(b) FROM df").df()

    create_df_res = create_df()
    simple_select_res = simple_select(create_df_res)
    add_col_res = add_col(simple_select_res)
    aggregate_res = aggregate(add_col_res)


duckdb_transform()
```

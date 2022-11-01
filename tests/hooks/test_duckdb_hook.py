import unittest
from unittest import mock

from duckdb_provider.hooks.duckdb_hook import DuckDBHook


@mock.patch.dict("os.environ", AIRFLOW_CONN_DUCKDB_DEFAULT="duckdb://:memory:")
class TestDuckDBHook(unittest.TestCase):
    """
    Test the DuckDB hook behavior. Mocks the duckdb_default connection.
    """

    def test_get_conn(self):
        """
        Test that the hook can get a connection
        """
        hook = DuckDBHook.get_hook("duckdb_default")
        conn = hook.get_conn()
        self.assertIsNotNone(conn)

    def test_execute(self):
        """
        Test that the hook can execute a query
        """
        hook = DuckDBHook.get_hook("duckdb_default")
        conn = hook.get_conn()
        res = conn.execute("SELECT 1").df()
        self.assertEqual(res, 1)

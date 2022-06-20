#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import sys
from pathlib import Path

# add parent directory
sys.path.append(Path(__file__).parent.parent.parent.parent.as_posix())
print(sys.path)
import openmldb

from parameterized import parameterized

import json
import os
import unittest
import uuid
from contextlib import closing
from unittest import mock

import pytest
# from parameterized import parameterized

from airflow.models import Connection
from airflow.models.dag import DAG
from airflow_provider_openmldb.hooks.openmldb import OpenMLDBHook
from airflow.utils import timezone


class TestMySqlHookConn(unittest.TestCase):
    def setUp(self):
        super().setUp()

        self.connection = Connection(
            # conn_type='openmldb',
            # login='login',
            # password='password',
            # host='host', # TODO(hw): not recommended
            # schema='demo_db',
            uri='openmldb:///fake?zk=foo&zkPath=/bar',
        )

        self.db_hook = OpenMLDBHook()
        self.db_hook.get_connection = mock.Mock()
        self.db_hook.get_connection.return_value = self.connection

    @mock.patch('openmldb.dbapi.dbapi.connect')
    def test_get_conn(self, mock_connect):
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        assert args == ('fake', 'foo', '/bar')
        assert kwargs == {}

    @mock.patch('openmldb.dbapi.dbapi.connect')
    def test_get_uri(self, mock_connect):
        # TODO(hw): zkSessionTimeout is not supported now
        self.connection.extra = json.dumps({'zk': 'a', 'zkPath': 'b', 'zkSessionTimeout': '12345'})
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        print(self.db_hook.get_uri())
        assert self.db_hook.get_uri() == "openmldb:///fake?zk=a&zkPath=b&zkSessionTimeout=12345"

    @mock.patch('openmldb.dbapi.dbapi.connect')
    def test_get_conn_from_connection(self, mock_connect):
        hook = OpenMLDBHook(connection=self.connection)
        hook.get_conn()
        mock_connect.assert_called_once_with('fake', 'foo', '/bar')

    @mock.patch('openmldb.dbapi.dbapi.connect')
    def test_get_conn_from_connection_with_schema_override(self, mock_connect):
        hook = OpenMLDBHook(connection=self.connection, schema='db-override')
        hook.get_conn()
        mock_connect.assert_called_once_with('db-override', 'foo', '/bar')


class MockMySQLConnectorConnection:
    DEFAULT_AUTOCOMMIT = 'default'

    def __init__(self):
        self._autocommit = self.DEFAULT_AUTOCOMMIT

    @property
    def autocommit(self):
        return self._autocommit

    @autocommit.setter
    def autocommit(self, autocommit):
        self._autocommit = autocommit


class TestOpenMLDBHook(unittest.TestCase):
    def setUp(self):
        super().setUp()

        self.cur = mock.MagicMock(rowcount=0)
        self.conn = mock.MagicMock()
        self.conn.cursor.return_value = self.cur
        conn = self.conn

        class SubOpenMLDBHook(OpenMLDBHook):
            conn_name_attr = 'test_conn_id'

            def get_conn(self):
                return conn

        self.db_hook = SubOpenMLDBHook()

    @parameterized.expand([(True,), (False,)])
    def test_set_autocommit_mysql_connector(self, autocommit):
        conn = MockMySQLConnectorConnection()
        self.db_hook.set_autocommit(conn, autocommit)
        assert False


#
#     def test_get_autocommit_mysql_connector(self):
#         conn = MockMySQLConnectorConnection()
#         assert self.db_hook.get_autocommit(conn) == MockMySQLConnectorConnection.DEFAULT_AUTOCOMMIT
#
#     def test_set_autocommit_mysqldb(self):
#         autocommit = False
#         self.db_hook.set_autocommit(self.conn, autocommit)
#         self.conn.autocommit.assert_called_once_with(autocommit)
#
#     def test_get_autocommit_mysqldb(self):
#         self.db_hook.get_autocommit(self.conn)
#         self.conn.get_autocommit.assert_called_once()
#
#     def test_run_without_autocommit(self):
#         sql = 'SQL'
#         self.conn.get_autocommit.return_value = False
#
#         # Default autocommit setting should be False.
#         # Testing default autocommit value as well as run() behavior.
#         self.db_hook.run(sql, autocommit=False)
#         self.conn.autocommit.assert_called_once_with(False)
#         self.cur.execute.assert_called_once_with(sql)
#         assert self.conn.commit.call_count == 1
#
#     def test_run_with_autocommit(self):
#         sql = 'SQL'
#         self.db_hook.run(sql, autocommit=True)
#         self.conn.autocommit.assert_called_once_with(True)
#         self.cur.execute.assert_called_once_with(sql)
#         self.conn.commit.assert_not_called()
#
#     def test_run_with_parameters(self):
#         sql = 'SQL'
#         parameters = ('param1', 'param2')
#         self.db_hook.run(sql, autocommit=True, parameters=parameters)
#         self.conn.autocommit.assert_called_once_with(True)
#         self.cur.execute.assert_called_once_with(sql, parameters)
#         self.conn.commit.assert_not_called()
#
#     def test_run_multi_queries(self):
#         sql = ['SQL1', 'SQL2']
#         self.db_hook.run(sql, autocommit=True)
#         self.conn.autocommit.assert_called_once_with(True)
#         for i, item in enumerate(self.cur.execute.call_args_list):
#             args, kwargs = item
#             assert len(args) == 1
#             assert args[0] == sql[i]
#             assert kwargs == {}
#         calls = [mock.call(sql[0]), mock.call(sql[1])]
#         self.cur.execute.assert_has_calls(calls, any_order=True)
#         self.conn.commit.assert_not_called()
#
#     def test_bulk_load(self):
#         self.db_hook.bulk_load('table', '/tmp/file')
#         self.cur.execute.assert_called_once_with(
#             """
#             LOAD DATA LOCAL INFILE '/tmp/file'
#             INTO TABLE table
#             """
#         )
#
#     def test_bulk_dump(self):
#         self.db_hook.bulk_dump('table', '/tmp/file')
#         self.cur.execute.assert_called_once_with(
#             """
#             SELECT * INTO OUTFILE '/tmp/file'
#             FROM table
#             """
#         )
#
#     def test_serialize_cell(self):
#         assert 'foo' == self.db_hook._serialize_cell('foo', None)
#
#     def test_bulk_load_custom(self):
#         self.db_hook.bulk_load_custom(
#             'table',
#             '/tmp/file',
#             'IGNORE',
#             """FIELDS TERMINATED BY ';'
#             OPTIONALLY ENCLOSED BY '"'
#             IGNORE 1 LINES""",
#         )
#         self.cur.execute.assert_called_once_with(
#             """
#             LOAD DATA LOCAL INFILE '/tmp/file'
#             IGNORE
#             INTO TABLE table
#             FIELDS TERMINATED BY ';'
#             OPTIONALLY ENCLOSED BY '"'
#             IGNORE 1 LINES
#             """
#         )


# DEFAULT_DATE = timezone.datetime(2015, 1, 1)
# DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
# DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]
# TEST_DAG_ID = 'unit_test_dag'
#
#
# class MySqlContext:
#     def __init__(self, client):
#         self.client = client
#         self.connection = MySqlHook.get_connection(MySqlHook.default_conn_name)
#         self.init_client = self.connection.extra_dejson.get('client', 'mysqlclient')
#
#     def __enter__(self):
#         self.connection.set_extra(f'{{"client": "{self.client}"}}')
#
#     def __exit__(self, exc_type, exc_val, exc_tb):
#         self.connection.set_extra(f'{{"client": "{self.init_client}"}}')
#
#
# @pytest.mark.backend("mysql")
# class TestMySql(unittest.TestCase):
#     def setUp(self):
#         args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
#         dag = DAG(TEST_DAG_ID, default_args=args)
#         self.dag = dag
#
#     def tearDown(self):
#         drop_tables = {'test_mysql_to_mysql', 'test_airflow'}
#         with closing(MySqlHook().get_conn()) as conn:
#             with closing(conn.cursor()) as cursor:
#                 for table in drop_tables:
#                     cursor.execute(f"DROP TABLE IF EXISTS {table}")
#
#     @parameterized.expand(
#         [
#             ("mysqlclient",),
#             ("mysql-connector-python",),
#         ]
#     )
#     @mock.patch.dict(
#         'os.environ',
#         {
#             'AIRFLOW_CONN_AIRFLOW_DB': 'mysql://root@mysql/airflow?charset=utf8mb4&local_infile=1',
#         },
#     )
#     def test_mysql_hook_test_bulk_load(self, client):
#         with MySqlContext(client):
#             records = ("foo", "bar", "baz")
#
#             import tempfile
#
#             with tempfile.NamedTemporaryFile() as f:
#                 f.write("\n".join(records).encode('utf8'))
#                 f.flush()
#
#                 hook = MySqlHook('airflow_db')
#                 with closing(hook.get_conn()) as conn:
#                     with closing(conn.cursor()) as cursor:
#                         cursor.execute(
#                             """
#                             CREATE TABLE IF NOT EXISTS test_airflow (
#                                 dummy VARCHAR(50)
#                             )
#                         """
#                         )
#                         cursor.execute("TRUNCATE TABLE test_airflow")
#                         hook.bulk_load("test_airflow", f.name)
#                         cursor.execute("SELECT dummy FROM test_airflow")
#                         results = tuple(result[0] for result in cursor.fetchall())
#                         assert sorted(results) == sorted(records)
#
#     @parameterized.expand(
#         [
#             ("mysqlclient",),
#             ("mysql-connector-python",),
#         ]
#     )
#     def test_mysql_hook_test_bulk_dump(self, client):
#         with MySqlContext(client):
#             hook = MySqlHook('airflow_db')
#             priv = hook.get_first("SELECT @@global.secure_file_priv")
#             # Use random names to allow re-running
#             if priv and priv[0]:
#                 # Confirm that no error occurs
#                 hook.bulk_dump(
#                     "INFORMATION_SCHEMA.TABLES",
#                     os.path.join(priv[0], f"TABLES_{client}-{uuid.uuid1()}"),
#                 )
#             elif priv == ("",):
#                 hook.bulk_dump("INFORMATION_SCHEMA.TABLES", f"TABLES_{client}_{uuid.uuid1()}")
#             else:
#                 raise pytest.skip("Skip test_mysql_hook_test_bulk_load since file output is not permitted")
#
#     @parameterized.expand(
#         [
#             ("mysqlclient",),
#             ("mysql-connector-python",),
#         ]
#     )
#     @mock.patch('airflow.providers.mysql.hooks.mysql.MySqlHook.get_conn')
#     def test_mysql_hook_test_bulk_dump_mock(self, client, mock_get_conn):
#         with MySqlContext(client):
#             mock_execute = mock.MagicMock()
#             mock_get_conn.return_value.cursor.return_value.execute = mock_execute
#
#             hook = MySqlHook('airflow_db')
#             table = "INFORMATION_SCHEMA.TABLES"
#             tmp_file = "/path/to/output/file"
#             hook.bulk_dump(table, tmp_file)
#
#             from tests.test_utils.asserts import assert_equal_ignore_multiple_spaces
#
#             assert mock_execute.call_count == 1
#             query = f"""
#                 SELECT * INTO OUTFILE '{tmp_file}'
#                 FROM {table}
#             """
#             assert_equal_ignore_multiple_spaces(self, mock_execute.call_args[0][0], query)

if __name__ == "__main__":
    sys.exit(unittest.main())

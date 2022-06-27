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
# fmt: off
import sys
from pathlib import Path

# add parent directory
sys.path.append(Path(__file__).parent.parent.parent.parent.as_posix())

from airflow_provider_openmldb.hooks.openmldb import OpenMLDBHook

from airflow.models import Connection
from airflow.models.dag import DAG
from airflow.utils import timezone
# fmt: on

from parameterized import parameterized

import json
import unittest
from contextlib import closing
from unittest import mock


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

    @mock.patch('openmldb.dbapi.connect')
    def test_get_conn(self, mock_connect):
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        assert args == ('fake', 'foo', '/bar')
        assert kwargs == {}

    @mock.patch('openmldb.dbapi.connect')
    def test_get_uri(self, mock_connect):
        # TODO(hw): zkSessionTimeout is not supported now
        self.connection.extra = json.dumps({'zk': 'a', 'zkPath': 'b', 'zkSessionTimeout': '12345'})
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        print(self.db_hook.get_uri())
        assert self.db_hook.get_uri() == "openmldb:///fake?zk=a&zkPath=b&zkSessionTimeout=12345"

    @mock.patch('openmldb.dbapi.connect')
    def test_get_conn_from_connection(self, mock_connect):
        hook = OpenMLDBHook(connection=self.connection)
        hook.get_conn()
        mock_connect.assert_called_once_with('fake', 'foo', '/bar')

    @mock.patch('openmldb.dbapi.connect')
    def test_get_conn_from_connection_with_schema_override(self, mock_connect):
        hook = OpenMLDBHook(connection=self.connection, schema='db-override')
        hook.get_conn()
        mock_connect.assert_called_once_with('db-override', 'foo', '/bar')


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
        self.db_hook.set_autocommit(self.conn, autocommit)
        assert self.db_hook.get_autocommit(self.conn) is False

    def test_set_autocommit(self):
        autocommit = False
        self.db_hook.set_autocommit(self.conn, autocommit)
        # supports_autocommit == False, won't set autocommit
        self.conn.set_autocommit.assert_not_called()

    def test_get_autocommit(self):
        self.db_hook.get_autocommit(self.conn)
        # supports_autocommit == False, won't get autocommit
        self.conn.get_autocommit.assert_not_called()

    @parameterized.expand([(True,), (False,)])
    def test_run_with_set_autocommit(self, autocommit):
        sql = 'SQL'
        # self.conn.get_autocommit.return_value = False

        # Default autocommit setting should be False.
        # Testing default autocommit value as well as run() behavior.
        self.db_hook.run(sql, autocommit=autocommit)
        # supports_autocommit == False, won't set autocommit
        self.conn.autocommit.assert_not_called()
        self.cur.execute.assert_called_once_with(sql)
        assert self.conn.commit.call_count == 1

    def test_run_with_parameters(self):
        sql = 'SQL'
        parameters = ('param1', 'param2')
        self.db_hook.run(sql, parameters=parameters)
        self.cur.execute.assert_called_once_with(sql, parameters)

    def test_run_multi_queries(self):
        sql = ['SQL1', 'SQL2']
        self.db_hook.run(sql)
        for i, item in enumerate(self.cur.execute.call_args_list):
            args, kwargs = item
            assert len(args) == 1
            assert args[0] == sql[i]
            assert kwargs == {}
        calls = [mock.call(sql[0]), mock.call(sql[1])]
        self.cur.execute.assert_has_calls(calls, any_order=True)

    def test_serialize_cell(self):
        assert 'foo' == self.db_hook._serialize_cell('foo', None)


DEFAULT_DATE = timezone.datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]
TEST_DAG_ID = 'unit_test_dag'


# Mock the Airflow connection
# airflow has default connections, ref create_default_connections. OpenMLDB connection should be manually created.
@mock.patch.dict('os.environ',
                 AIRFLOW_CONN_OPENMLDB_DB='openmldb:///fake?zk=127.0.0.1:2181&zkPath=/openmldb&zkSessionTimeout=60000')
class TestOpenMLDB(unittest.TestCase):
    def setUp(self):
        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        dag = DAG(TEST_DAG_ID, default_args=args)
        self.dag = dag

    def tearDown(self):
        pass
        # drop_tables = {'test_airflow'}
        # with closing(OpenMLDBHook('airflow_db').get_conn()) as conn:
        #     with closing(conn.cursor()) as cursor:
        #         for table in drop_tables:
        #             cursor.execute(f"DROP TABLE IF EXISTS {table}")

    def test_smoke(self):
        hook = OpenMLDBHook('openmldb_db')
        hook.get_conn()

    @unittest.skip
    def test_mysql_hook_test_bulk_load(self):
        records = ("foo", "bar", "baz")

        import tempfile

        with tempfile.NamedTemporaryFile() as f:
            f.write("\n".join(records).encode('utf8'))
            f.flush()
            # TODO(hw): conn_name_attr just a string, how to connect the right database system?
            hook = OpenMLDBHook('airflow_db')
            with closing(hook.get_conn()) as conn:
                with closing(conn.cursor()) as cursor:
                    cursor.execute(
                        """
                        CREATE TABLE IF NOT EXISTS test_airflow (
                            dummy VARCHAR(50)
                        )
                    """
                    )
                    cursor.execute("TRUNCATE TABLE test_airflow")  # TODO(hw): unsupported
                    # hook.bulk_load("test_airflow", f.name) # TODO(hw): load infile?
                    cursor.execute("SELECT dummy FROM test_airflow")
                    # results = tuple(result[0] for result in cursor.fetchall())
                    # assert sorted(results) == sorted(records)

    @unittest.skip
    def test_mysql_hook_test_bulk_dump(self):
        hook = OpenMLDBHook('airflow_db')
        # hook.bulk_dump("INFORMATION_SCHEMA.TABLES", f"TABLES_{client}_{uuid.uuid1()}") # TODO(hw): select into


if __name__ == "__main__":
    sys.exit(unittest.main())

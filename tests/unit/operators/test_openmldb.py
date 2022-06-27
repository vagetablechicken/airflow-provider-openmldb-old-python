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


# fmt: off
# pylint: disable=unused-import
import os
import sys
from pathlib import Path

from openmldb.dbapi import DatabaseError

sys.path.append(Path(__file__).parent.parent.parent.parent.as_posix())

import openmldb

from airflow_provider_openmldb.hooks.openmldb import OpenMLDBHook
from airflow_provider_openmldb.operators.openmldb import OpenMLDBOperator

from airflow.models.dag import DAG
from airflow.utils import timezone

import unittest
from unittest import mock

# fmt: on

DEFAULT_DATE = timezone.datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]
TEST_DAG_ID = 'unit_test_dag'


@mock.patch.dict('os.environ', AIRFLOW_CONN_OPENMLDB_DEFAULT='openmldb:///test_db?zk=127.0.0.1:2181&zkPath=/openmldb'
                                                             '&zkSessionTimeout=60000')
class TestOpenMLDB(unittest.TestCase):
    # setup needs patch first
    @mock.patch.dict('os.environ', AIRFLOW_CONN_OPENMLDB_DEFAULT='openmldb:///?zk=127.0.0.1:2181&zkPath=/openmldb'
                                                                 '&zkSessionTimeout=60000')
    def setUp(self):
        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        dag = DAG(TEST_DAG_ID, default_args=args)
        self.dag = dag
        # create database first
        print(f'setup {os.environ.get("AIRFLOW_CONN_OPENMLDB_DEFAULT")}')
        OpenMLDBHook().run('CREATE DATABASE IF NOT EXISTS test_db')

    def tearDown(self):
        drop_tables = {'test_airflow'}
        # with closing(OpenMLDBHook().get_conn()) as conn:
        #     with closing(conn.cursor()) as cursor:
        #         for table in drop_tables:
        #             cursor.execute(f"DROP TABLE IF EXISTS {table}")

    def test_openmldb_operator_test(self):
        print(f'test hw {os.environ.get("AIRFLOW_CONN_OPENMLDB_DB")}')
        sql = """
        CREATE TABLE IF NOT EXISTS test_airflow (
            dummy VARCHAR(50)
        );
        """
        op = OpenMLDBOperator(task_id='basic_openmldb', openmldb_conn_id='openmldb_default', sql=sql, dag=self.dag)
        # dag test needs airflow server, do not do Operator.run
        # op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
        op.execute(context={})

    @unittest.skip
    def test_mysql_operator_test_multi(self):
        """multi sql is unsupported now"""
        sql = [
            "CREATE TABLE IF NOT EXISTS test_airflow (dummy VARCHAR(50))",
            # "TRUNCATE TABLE test_airflow", # unsupported
            "INSERT INTO test_airflow VALUES ('X')",
        ]
        op = OpenMLDBOperator(
            task_id='mysql_operator_test_multi',
            # openmldb_conn_id='openmldb_default',
            sql=sql,
            dag=self.dag,
        )
        op.execute(context={})

    def test_overwrite_schema(self):
        """
        Verifies option to overwrite connection schema
        SELECT 1; is ok even the database is not exists
        """
        sql = "SELECT 1;"
        op = OpenMLDBOperator(
            task_id='test_mysql_operator_test_schema_overwrite',
            sql=sql,
            dag=self.dag,
            database="foobar",
        )
        op.execute(context={})

    def test_overwrite_schema_1(self):
        """
        Verifies option to overwrite connection schema
        """
        sql = "SELECT * FROM test_airflow;"
        op = OpenMLDBOperator(
            task_id='test_mysql_operator_test_schema_overwrite',
            sql=sql,
            dag=self.dag,
            database="foobar",
        )

        try:
            op.execute(context={})
        except DatabaseError as e:
            assert "table test_airflow not exists in database [foobar]" in str(e)

    # TODO(hw): check sql with parameters


if __name__ == "__main__":
    sys.exit(unittest.main())

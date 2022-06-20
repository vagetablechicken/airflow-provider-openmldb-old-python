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

"""This module allows to connect to a MySQL database."""
import openmldb
import openmldb.dbapi.dbapi as dbapi
import json
from typing import Dict, Optional, Tuple

from airflow.hooks.dbapi import DbApiHook
from airflow.models import Connection


class OpenMLDBHook(DbApiHook):
    """
    Interact with OpenMLDB.

    You can specify charset in the extra field of your connection
    as ``{"charset": "utf8"}``. Also you can choose cursor as
    ``{"cursor": "SSCursor"}``. Refer to the MySQLdb.cursors for more details.

    :param schema: The MySQL database schema to connect to.
    :param connection: The :ref:`MySQL connection id <howto/connection:mysql>` used for MySQL credentials.
    """

    conn_name_attr = 'mysql_conn_id'
    default_conn_name = 'openmldb_default'
    conn_type = 'openmldb'
    hook_name = 'OpenMLDB'
    supports_autocommit = False

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.schema = kwargs.pop("schema", None)
        self.connection = kwargs.pop("connection", None)

    def _parse_zk_options(self, zk) -> Dict:
        d = {}
        if zk.startswith('{'):
            # json style
            return json.loads(zk)
        else:
            # host style: zkWithPort/zkPath
            return d

    def _get_conn_config(self, conn: Connection) -> Dict:
        conn_config = {'db': self.schema or conn.schema or ''}
        zk_options = conn.extra or conn.host or 'localhost:2181/openmldb'
        # zk zk path for dbapi connection
        print('input', self._parse_zk_options(zk_options))
        conn_config.update(self._parse_zk_options(zk_options))
        print('parsed conf', conn_config)
        return conn_config

    def get_conn(self) -> dbapi.Connection:
        """
        Establishes a connection to an openmldb database
        by extracting the connection configuration from the Airflow connection.

        .. note::

        :return: a openmldb dbapi connection object
        """
        conn = self.connection or self.get_connection(getattr(self, self.conn_name_attr))

        conn_config = self._get_conn_config(conn)
        # TODO(hw): standalone?
        return dbapi.connect(conn_config['db'], conn_config['zk'], conn_config['zkPath'])

        raise ValueError('Unknown OpenMLDB client name provided!')

    def bulk_load(self, table: str, tmp_file: str) -> None:
        """Loads a tab-delimited file into a database table"""
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute(
            f"""
            LOAD DATA LOCAL INFILE '{tmp_file}'
            INTO TABLE {table}
            """
        )
        conn.commit()

    def bulk_dump(self, table: str, tmp_file: str) -> None:
        """Dumps a database table into a tab-delimited file"""
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT * INTO OUTFILE '{tmp_file}'
            FROM {table}
            """
        )
        conn.commit()

    @staticmethod
    def _serialize_cell(cell: object, conn: Optional[Connection] = None) -> object:
        """
        The package OpenMLDB converts an argument to a literal
        when passing those separately to execute. Hence, this method does nothing.

        :param cell: The cell to insert into the table
        :param conn: The database connection
        :return: The same cell
        :rtype: object
        """
        return cell

    def bulk_load_custom(
            self, table: str, tmp_file: str, duplicate_key_handling: str = 'IGNORE', extra_options: str = ''
    ) -> None:
        """
        A more configurable way to load local data from a file into the database.

        .. warning:: According to the mysql docs using this function is a
            `security risk <https://dev.mysql.com/doc/refman/8.0/en/load-data-local.html>`_.
            If you want to use it anyway you can do so by setting a client-side + server-side option.
            This depends on the mysql client library used.

        :param table: The table were the file will be loaded into.
        :param tmp_file: The file (name) that contains the data.
        :param duplicate_key_handling: Specify what should happen to duplicate data.
            You can choose either `IGNORE` or `REPLACE`.

            .. seealso::
                https://dev.mysql.com/doc/refman/8.0/en/load-data.html#load-data-duplicate-key-handling
        :param extra_options: More sql options to specify exactly how to load the data.

            .. seealso:: https://dev.mysql.com/doc/refman/8.0/en/load-data.html
        """
        conn = self.get_conn()
        cursor = conn.cursor()

        cursor.execute(
            f"""
            LOAD DATA LOCAL INFILE '{tmp_file}'
            {duplicate_key_handling}
            INTO TABLE {table}
            {extra_options}
            """
        )

        cursor.close()
        conn.commit()

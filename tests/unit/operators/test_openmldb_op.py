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

import logging
import unittest
from unittest import mock

# Import Operator
from airflow_provider_openmldb.operators.openmldb import OpenMLDBOperator
from airflow_provider_openmldb.shared_utils import no_op

log = logging.getLogger(__name__)


# Mock the `conn_sample` Airflow connection
@mock.patch.dict("os.environ", AIRFLOW_CONN_KAFKA_SAMPLE="localhost:9092")
class TestOpenMLDBOp(unittest.TestCase):
    """
    Test OpenMLDB Operator
    """

    def test_operator(self):

        operator = OpenMLDBOperator(
            topics=["test"],
            apply_function="airflow_provider_openmldb.shared_utils.no_op",
            consumer_config={"socket.timeout.ms": 10, "group.id": "test", "bootstrap.servers": "test"},
            task_id="test",
            poll_timeout=0.0001,
        )

        # execute the operator (this is essentially a no op as the broker isn't setup)
        operator.execute(context={})

    def test_operator_callable(self):

        operator = ConsumeFromTopicOperator(
            topics=["test"],
            apply_function=no_op,
            consumer_config={"socket.timeout.ms": 10, "group.id": "test", "bootstrap.servers": "test"},
            task_id="test",
            poll_timeout=0.0001,
        )

        # execute the operator (this is essentially a no op as the broker isn't setup)
        operator.execute(context={})


if __name__ == "__main__":
    unittest.main()

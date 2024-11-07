# Copyright 2020 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os
import uuid

import pytest

from . import hello_world_write
from ..utils import create_table_cm

PROJECT = os.environ["GOOGLE_CLOUD_PROJECT"]
BIGTABLE_INSTANCE = os.environ["BIGTABLE_INSTANCE"]
TABLE_ID = f"mobile-time-series-beam-{str(uuid.uuid4())[:16]}"


@pytest.fixture(scope="module", autouse=True)
def table():
    with create_table_cm(
        PROJECT, BIGTABLE_INSTANCE, TABLE_ID, {"stats_summary": None}
    ) as table:
        yield table


def test_hello_world_write(table):
    hello_world_write.run(
        [
            "--bigtable-project=%s" % PROJECT,
            "--bigtable-instance=%s" % BIGTABLE_INSTANCE,
            "--bigtable-table=%s" % TABLE_ID,
        ]
    )

    rows = table.read_rows()
    count = 0
    for _ in rows:
        count += 1
    assert count == 2

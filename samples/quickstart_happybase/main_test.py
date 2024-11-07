# Copyright 2018 Google Inc.
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

from .main import main
from ..utils import create_table_cm

PROJECT = os.environ["GOOGLE_CLOUD_PROJECT"]
BIGTABLE_INSTANCE = os.environ["BIGTABLE_INSTANCE"]
TABLE_ID = f"quickstart-hb-test-{str(uuid.uuid4())[:16]}"


@pytest.fixture()
def table():
    column_family_id = "cf1"
    column_families = {column_family_id: None}
    with create_table_cm(PROJECT, BIGTABLE_INSTANCE, TABLE_ID, column_families) as table:
        row = table.direct_row("r1")
        row.set_cell(column_family_id, "c1", "test-value")
        row.commit()

        yield TABLE_ID


def test_main(capsys, table):
    table_id = table
    main(PROJECT, BIGTABLE_INSTANCE, table_id)

    out, _ = capsys.readouterr()
    assert "Row key: r1\nData: test-value\n" in out

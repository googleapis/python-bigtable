# Copyright 2018 Google LLC
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


import mock
import time

import pytest

from google.cloud.bigtable.row import DirectRow
from google.cloud.bigtable.batcher import MutationsBatcher

TABLE_ID = "table-id"
TABLE_NAME = "/tables/" + TABLE_ID


def test_mutation_batcher_constructor():
    table = _Table(TABLE_NAME)
    with MutationsBatcher(table) as mutation_batcher:
        assert table is mutation_batcher.table


def test_mutation_batcher_mutate_row():
    table = _Table(TABLE_NAME)
    with MutationsBatcher(table=table) as mutation_batcher:

        rows = [
            DirectRow(row_key=b"row_key"),
            DirectRow(row_key=b"row_key_2"),
            DirectRow(row_key=b"row_key_3"),
            DirectRow(row_key=b"row_key_4"),
        ]

        mutation_batcher.mutate_rows(rows)

    assert table.mutation_calls == 1


def test_mutation_batcher_mutate():
    table = _Table(TABLE_NAME)
    with MutationsBatcher(table=table) as mutation_batcher:

        row = DirectRow(row_key=b"row_key")
        row.set_cell("cf1", b"c1", 1)
        row.set_cell("cf1", b"c2", 2)
        row.set_cell("cf1", b"c3", 3)
        row.set_cell("cf1", b"c4", 4)

        mutation_batcher.mutate(row)

    assert table.mutation_calls == 1


def test_mutation_batcher_flush_w_no_rows():
    table = _Table(TABLE_NAME)
    with MutationsBatcher(table=table) as mutation_batcher:
        mutation_batcher.flush()

    assert table.mutation_calls == 0


def test_mutation_batcher_mutate_w_max_flush_count():
    table = _Table(TABLE_NAME)
    with MutationsBatcher(table=table, flush_count=3) as mutation_batcher:

        row_1 = DirectRow(row_key=b"row_key_1")
        row_2 = DirectRow(row_key=b"row_key_2")
        row_3 = DirectRow(row_key=b"row_key_3")

        mutation_batcher.mutate(row_1)
        mutation_batcher.mutate(row_2)
        mutation_batcher.mutate(row_3)

    assert table.mutation_calls == 1


@mock.patch("google.cloud.bigtable.batcher.MAX_MUTATIONS", new=3)
def test_mutation_batcher_mutate_with_max_mutations_failure():
    from google.cloud.bigtable.batcher import MaxMutationsError

    table = _Table(TABLE_NAME)
    with MutationsBatcher(table=table) as mutation_batcher:

        row = DirectRow(row_key=b"row_key")
        row.set_cell("cf1", b"c1", 1)
        row.set_cell("cf1", b"c2", 2)
        row.set_cell("cf1", b"c3", 3)
        row.set_cell("cf1", b"c4", 4)

        with pytest.raises(MaxMutationsError):
            mutation_batcher.mutate(row)


@mock.patch("google.cloud.bigtable.batcher.MAX_MUTATIONS", new=3)
def test_mutation_batcher_mutate_w_max_mutations():
    table = _Table(TABLE_NAME)
    with MutationsBatcher(table=table) as mutation_batcher:

        row = DirectRow(row_key=b"row_key")
        row.set_cell("cf1", b"c1", 1)
        row.set_cell("cf1", b"c2", 2)
        row.set_cell("cf1", b"c3", 3)

        mutation_batcher.mutate(row)

    assert table.mutation_calls == 1


def test_mutation_batcher_mutate_w_max_row_bytes():
    table = _Table(TABLE_NAME)
    with MutationsBatcher(
        table=table, max_row_bytes=3 * 1024 * 1024
    ) as mutation_batcher:

        number_of_bytes = 1 * 1024 * 1024
        max_value = b"1" * number_of_bytes

        row = DirectRow(row_key=b"row_key")
        row.set_cell("cf1", b"c1", max_value)
        row.set_cell("cf1", b"c2", max_value)
        row.set_cell("cf1", b"c3", max_value)

        mutation_batcher.mutate(row)

    assert table.mutation_calls == 1


def test_mutations_batcher_flushed_when_closed():
    table = _Table(TABLE_NAME)
    mutation_batcher = MutationsBatcher(table=table, max_row_bytes=3 * 1024 * 1024)

    number_of_bytes = 1 * 1024 * 1024
    max_value = b"1" * number_of_bytes

    row = DirectRow(row_key=b"row_key")
    row.set_cell("cf1", b"c1", max_value)
    row.set_cell("cf1", b"c2", max_value)

    mutation_batcher.mutate(row)
    assert table.mutation_calls == 0

    mutation_batcher.close()

    assert table.mutation_calls == 1


def test_mutations_batcher_context_manager_flushed_when_closed():
    table = _Table(TABLE_NAME)
    with MutationsBatcher(
        table=table, max_row_bytes=3 * 1024 * 1024
    ) as mutation_batcher:

        number_of_bytes = 1 * 1024 * 1024
        max_value = b"1" * number_of_bytes

        row = DirectRow(row_key=b"row_key")
        row.set_cell("cf1", b"c1", max_value)
        row.set_cell("cf1", b"c2", max_value)

        mutation_batcher.mutate(row)

    assert table.mutation_calls == 1


def test_mutations_batcher_mutate_after_batcher_closed_raise_error():
    from google.cloud.bigtable.batcher import BatcherIsClosedError

    table = _Table(TABLE_NAME)
    mutation_batcher = MutationsBatcher(table=table)
    mutation_batcher.close()

    assert table.mutation_calls == 0
    with pytest.raises(BatcherIsClosedError):
        row = DirectRow(row_key=b"row_key")
        row.set_cell("cf1", b"c1", 1)
        mutation_batcher.mutate(row)


@mock.patch("google.cloud.bigtable.batcher.MutationsBatcher.flush")
def test_mutations_batcher_flush_interval(mocked_flush):
    table = _Table(TABLE_NAME)
    flush_interval = 0.5
    mutation_batcher = MutationsBatcher(table=table, flush_interval=flush_interval)

    assert mutation_batcher._timer.interval == flush_interval
    mocked_flush.assert_not_called()

    time.sleep(0.4)
    mocked_flush.assert_not_called()

    time.sleep(0.1)
    mocked_flush.assert_called_once_with()

    mutation_batcher.close()


class _Instance(object):
    def __init__(self, client=None):
        self._client = client


class _Table(object):
    def __init__(self, name, client=None):
        self.name = name
        self._instance = _Instance(client)
        self.mutation_calls = 0

    def mutate_rows(self, rows):
        self.mutation_calls += 1
        return rows

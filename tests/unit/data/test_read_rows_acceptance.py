# Copyright 2023 Google LLC
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
from __future__ import annotations

import os
from itertools import zip_longest

import pytest
import mock

from google.cloud.bigtable_v2 import ReadRowsResponse

from google.cloud.bigtable.data._async.client import BigtableDataClientAsync
from google.cloud.bigtable.data.exceptions import InvalidChunk
from google.cloud.bigtable.data._async._read_rows import _ReadRowsOperationAsync
from google.cloud.bigtable.data.row import Row

from ..v2_client.test_row_merger import ReadRowsTest, TestFile


def parse_readrows_acceptance_tests():
    dirname = os.path.dirname(__file__)
    filename = os.path.join(dirname, "./read-rows-acceptance-test.json")

    with open(filename) as json_file:
        test_json = TestFile.from_json(json_file.read())
        return test_json.read_rows_tests


def extract_results_from_row(row: Row):
    results = []
    for family, col, cells in row.items():
        for cell in cells:
            results.append(
                ReadRowsTest.Result(
                    row_key=row.row_key,
                    family_name=family,
                    qualifier=col,
                    timestamp_micros=cell.timestamp_ns // 1000,
                    value=cell.value,
                    label=(cell.labels[0] if cell.labels else ""),
                )
            )
    return results


@pytest.mark.parametrize(
    "test_case", parse_readrows_acceptance_tests(), ids=lambda t: t.description
)
@pytest.mark.asyncio
async def test_row_merger_scenario(test_case: ReadRowsTest):
    async def _scenerio_stream():
        for chunk in test_case.chunks:
            yield ReadRowsResponse(chunks=[chunk])

    try:
        results = []
        instance = mock.Mock()
        instance._last_yielded_row_key = None
        instance._remaining_count = None
        chunker = _ReadRowsOperationAsync.chunk_stream(
            instance, _coro_wrapper(_scenerio_stream())
        )
        merger = _ReadRowsOperationAsync.merge_rows(chunker)
        async for row in merger:
            for cell in row:
                cell_result = ReadRowsTest.Result(
                    row_key=cell.row_key,
                    family_name=cell.family,
                    qualifier=cell.qualifier,
                    timestamp_micros=cell.timestamp_micros,
                    value=cell.value,
                    label=cell.labels[0] if cell.labels else "",
                )
                results.append(cell_result)
    except InvalidChunk:
        results.append(ReadRowsTest.Result(error=True))
    for expected, actual in zip_longest(test_case.results, results):
        assert actual == expected


@pytest.mark.parametrize(
    "test_case", parse_readrows_acceptance_tests(), ids=lambda t: t.description
)
@pytest.mark.asyncio
async def test_read_rows_scenario(test_case: ReadRowsTest):
    async def _make_gapic_stream(chunk_list: list[ReadRowsResponse]):
        from google.cloud.bigtable_v2 import ReadRowsResponse

        class mock_stream:
            def __init__(self, chunk_list):
                self.chunk_list = chunk_list
                self.idx = -1

            def __aiter__(self):
                return self

            async def __anext__(self):
                self.idx += 1
                if len(self.chunk_list) > self.idx:
                    chunk = self.chunk_list[self.idx]
                    return ReadRowsResponse(chunks=[chunk])
                raise StopAsyncIteration

            def cancel(self):
                pass

        return mock_stream(chunk_list)

    try:
        with mock.patch.dict(os.environ, {"BIGTABLE_EMULATOR_HOST": "localhost"}):
            # use emulator mode to avoid auth issues in CI
            client = BigtableDataClientAsync()
        table = client.get_table("instance", "table")
        results = []
        with mock.patch.object(table.client._gapic_client, "read_rows") as read_rows:
            # run once, then return error on retry
            read_rows.return_value = _make_gapic_stream(test_case.chunks)
            async for row in await table.read_rows_stream(query={}):
                for cell in row:
                    cell_result = ReadRowsTest.Result(
                        row_key=cell.row_key,
                        family_name=cell.family,
                        qualifier=cell.qualifier,
                        timestamp_micros=cell.timestamp_micros,
                        value=cell.value,
                        label=cell.labels[0] if cell.labels else "",
                    )
                    results.append(cell_result)
    except InvalidChunk:
        results.append(ReadRowsTest.Result(error=True))
    finally:
        await client.close()
    for expected, actual in zip_longest(test_case.results, results):
        assert actual == expected


@pytest.mark.asyncio
async def test_out_of_order_rows():
    async def _row_stream():
        yield ReadRowsResponse(last_scanned_row_key=b"a")

    instance = mock.Mock()
    instance._remaining_count = None
    instance._last_yielded_row_key = b"b"
    chunker = _ReadRowsOperationAsync.chunk_stream(
        instance, _coro_wrapper(_row_stream())
    )
    merger = _ReadRowsOperationAsync.merge_rows(chunker)
    with pytest.raises(InvalidChunk):
        async for _ in merger:
            pass


@pytest.mark.asyncio
async def test_bare_reset():
    first_chunk = ReadRowsResponse.CellChunk(
        ReadRowsResponse.CellChunk(
            row_key=b"a", family_name="f", qualifier=b"q", value=b"v"
        )
    )
    with pytest.raises(InvalidChunk):
        await _process_chunks(
            first_chunk,
            ReadRowsResponse.CellChunk(
                ReadRowsResponse.CellChunk(reset_row=True, row_key=b"a")
            ),
        )
    with pytest.raises(InvalidChunk):
        await _process_chunks(
            first_chunk,
            ReadRowsResponse.CellChunk(
                ReadRowsResponse.CellChunk(reset_row=True, family_name="f")
            ),
        )
    with pytest.raises(InvalidChunk):
        await _process_chunks(
            first_chunk,
            ReadRowsResponse.CellChunk(
                ReadRowsResponse.CellChunk(reset_row=True, qualifier=b"q")
            ),
        )
    with pytest.raises(InvalidChunk):
        await _process_chunks(
            first_chunk,
            ReadRowsResponse.CellChunk(
                ReadRowsResponse.CellChunk(reset_row=True, timestamp_micros=1000)
            ),
        )
    with pytest.raises(InvalidChunk):
        await _process_chunks(
            first_chunk,
            ReadRowsResponse.CellChunk(
                ReadRowsResponse.CellChunk(reset_row=True, labels=["a"])
            ),
        )
    with pytest.raises(InvalidChunk):
        await _process_chunks(
            first_chunk,
            ReadRowsResponse.CellChunk(
                ReadRowsResponse.CellChunk(reset_row=True, value=b"v")
            ),
        )


@pytest.mark.asyncio
async def test_missing_family():
    with pytest.raises(InvalidChunk):
        await _process_chunks(
            ReadRowsResponse.CellChunk(
                row_key=b"a",
                qualifier=b"q",
                timestamp_micros=1000,
                value=b"v",
                commit_row=True,
            )
        )


@pytest.mark.asyncio
async def test_mid_cell_row_key_change():
    with pytest.raises(InvalidChunk):
        await _process_chunks(
            ReadRowsResponse.CellChunk(
                row_key=b"a",
                family_name="f",
                qualifier=b"q",
                timestamp_micros=1000,
                value_size=2,
                value=b"v",
            ),
            ReadRowsResponse.CellChunk(row_key=b"b", value=b"v", commit_row=True),
        )


@pytest.mark.asyncio
async def test_mid_cell_family_change():
    with pytest.raises(InvalidChunk):
        await _process_chunks(
            ReadRowsResponse.CellChunk(
                row_key=b"a",
                family_name="f",
                qualifier=b"q",
                timestamp_micros=1000,
                value_size=2,
                value=b"v",
            ),
            ReadRowsResponse.CellChunk(family_name="f2", value=b"v", commit_row=True),
        )


@pytest.mark.asyncio
async def test_mid_cell_qualifier_change():
    with pytest.raises(InvalidChunk):
        await _process_chunks(
            ReadRowsResponse.CellChunk(
                row_key=b"a",
                family_name="f",
                qualifier=b"q",
                timestamp_micros=1000,
                value_size=2,
                value=b"v",
            ),
            ReadRowsResponse.CellChunk(qualifier=b"q2", value=b"v", commit_row=True),
        )


@pytest.mark.asyncio
async def test_mid_cell_timestamp_change():
    with pytest.raises(InvalidChunk):
        await _process_chunks(
            ReadRowsResponse.CellChunk(
                row_key=b"a",
                family_name="f",
                qualifier=b"q",
                timestamp_micros=1000,
                value_size=2,
                value=b"v",
            ),
            ReadRowsResponse.CellChunk(
                timestamp_micros=2000, value=b"v", commit_row=True
            ),
        )


@pytest.mark.asyncio
async def test_mid_cell_labels_change():
    with pytest.raises(InvalidChunk):
        await _process_chunks(
            ReadRowsResponse.CellChunk(
                row_key=b"a",
                family_name="f",
                qualifier=b"q",
                timestamp_micros=1000,
                value_size=2,
                value=b"v",
            ),
            ReadRowsResponse.CellChunk(labels=["b"], value=b"v", commit_row=True),
        )


async def _coro_wrapper(stream):
    return stream


async def _process_chunks(*chunks):
    async def _row_stream():
        yield ReadRowsResponse(chunks=chunks)

    instance = mock.Mock()
    instance._remaining_count = None
    instance._last_yielded_row_key = None
    chunker = _ReadRowsOperationAsync.chunk_stream(
        instance, _coro_wrapper(_row_stream())
    )
    merger = _ReadRowsOperationAsync.merge_rows(chunker)
    results = []
    async for row in merger:
        results.append(row)
    return results

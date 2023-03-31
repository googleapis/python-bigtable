import os
from itertools import zip_longest

import proto
import pytest

from google.cloud.bigtable_v2 import ReadRowsResponse

from google.cloud.bigtable.row_merger import RowMerger, InvalidChunk
from google.cloud.bigtable.row_response import RowResponse

from .v2_client.test_row_merger import ReadRowsTest, TestFile

def parse_readrows_acceptance_tests():
    dirname = os.path.dirname(__file__)
    filename = os.path.join(dirname, "./read-rows-acceptance-test.json")

    with open(filename) as json_file:
        test_json = TestFile.from_json(json_file.read())
        return test_json.read_rows_tests


def extract_results_from_row(row: RowResponse):
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
async def test_scenario(test_case: ReadRowsTest):
    async def _scenerio_stream():
        for chunk in test_case.chunks:
            yield ReadRowsResponse(chunks=[chunk])

    try:
        merger = RowMerger()
        results = []
        async for row in merger.merge_row_stream(_scenerio_stream()):
            for cell in row:
                cell_result = ReadRowsTest.Result(
                    row_key=cell.row_key,
                    family_name=cell.family,
                    qualifier=cell.column_qualifier,
                    timestamp_micros=cell.timestamp_micros,
                    value=cell.value,
                    label=cell.labels[0] if cell.labels else "",
                )
                results.append(cell_result)
        if not merger.state_machine.is_terminal_state():
            raise InvalidChunk("merger has partial frame after reading")
    except InvalidChunk:
        results.append(ReadRowsTest.Result(error=True))
    for expected, actual in zip_longest(test_case.results, results):
        assert actual == expected


@pytest.mark.asyncio
async def test_out_of_order_rows():
    async def _row_stream():
        yield ReadRowsResponse(last_scanned_row_key=b"a")

    merger = RowMerger()
    merger.state_machine.last_seen_row_key = b"a"
    with pytest.raises(InvalidChunk):
        async for _ in merger.merge_row_stream(_row_stream()):
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


async def _process_chunks(*chunks):
    async def _row_stream():
        yield ReadRowsResponse(chunks=chunks)

    merger = RowMerger()
    results = []
    async for row in merger.merge_row_stream(_row_stream()):
        results.append(row)
    return results

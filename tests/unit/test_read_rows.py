import os
from itertools import zip_longest

import proto
import pytest

from google.cloud.bigtable_v2 import ReadRowsResponse

from google.cloud.bigtable.row_merger import RowMerger, InvalidChunk
from google.cloud.bigtable.row_response import RowResponse


# TODO: autogenerate protos from
#  https://github.com/googleapis/conformance-tests/blob/main/bigtable/v2/proto/google/cloud/conformance/bigtable/v2/tests.proto
class ReadRowsTest(proto.Message):
    class Result(proto.Message):
        row_key = proto.Field(proto.STRING, number=1)
        family_name = proto.Field(proto.STRING, number=2)
        qualifier = proto.Field(proto.STRING, number=3)
        timestamp_micros = proto.Field(proto.INT64, number=4)
        value = proto.Field(proto.STRING, number=5)
        label = proto.Field(proto.STRING, number=6)
        error = proto.Field(proto.BOOL, number=7)

    description = proto.Field(proto.STRING, number=1)
    chunks = proto.RepeatedField(
        proto.MESSAGE, number=2, message=ReadRowsResponse.CellChunk
    )
    results = proto.RepeatedField(proto.MESSAGE, number=3, message=Result)


class TestFile(proto.Message):
    __test__ = False
    read_rows_tests = proto.RepeatedField(proto.MESSAGE, number=1, message=ReadRowsTest)


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
            results.append(row)
        if not merger.state_machine.is_terminal_state():
            raise InvalidChunk("merger has partial frame after reading")
    except InvalidChunk:
        results.append(ReadRowsTest.Result(error=True))
    for expected, actual in zip_longest(test_case.results, results):
        assert actual == expected
    # def fake_read(*args, **kwargs):
    #     return iter([ReadRowsResponse(chunks=test_case.chunks)])
    # actual_results: List[ReadRowsTest.Result] = []
    # try:
    #     for row in PartialRowsData(fake_read, request=None):
    #         actual_results.extend(extract_results_from_row(row))
    # except (InvalidChunk, ValueError):
    #     actual_results.append(ReadRowsTest.Result(error=True))
    # breakpoint()


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

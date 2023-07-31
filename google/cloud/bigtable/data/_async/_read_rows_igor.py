import dataclasses

class InvalidChunk(Exception):
    pass

@dataclasses.dataclass
class Cell:
    family: str
    qualifier: bytes
    timestamp: int
    labels: list[str]
    value: bytes
@dataclasses.dataclass
class Row:
    key: bytes
    cells: list[Cell]

class _ResetRow(Exception):
    pass

async def start_operation(table, query):
    request = query._to_dict()
    request["table_name"] = table.table_name
    if table.app_profile_id:
        request["app_profile_id"] = table.app_profile_id
    s = await table.client._gapic_client.read_rows(request)
    s = chunk_stream(s)
    return [r async for r in merge_rows(s)]

async def chunk_stream(stream):
    prev_key = None

    async for resp in stream:
        resp = resp._pb

        if resp.last_scanned_row_key:
            if prev_key is not None and resp.last_scanned_row_key >= prev_key:
                raise InvalidChunk("last scanned out of order")
            prev_key = resp.last_scanned_row_key

        current_key = None

        for c in resp.chunks:
            if current_key is None:
                current_key = c.row_key
                if current_key is None:
                    raise InvalidChunk("first chunk is missing a row key")
                elif prev_key and current_key <= prev_key:
                    raise InvalidChunk("out of order row key")

            yield c

            if c.reset_row:
                current_key = None
            elif c.commit_row:
                prev_key = current_key

async def merge_rows(chunks):
    it = chunks.__aiter__()
    # import pdb; pdb.set_trace()

    # For each row
    try:
        while True:
            c = await it.__anext__()

            # EOS
            if c is None:
                return

            if c.reset_row:
                continue

            row_key = c.row_key

            if not row_key:
                raise InvalidChunk("first row chunk is missing key")

            # Cells
            cells = []

            # shared per cell storage
            family = c.family_name
            qualifier = c.qualifier

            try:
                # for each cell
                while True:
                    f = c.family_name
                    q = c.qualifier
                    if f:
                        family = f
                        qualifier = q
                    if q:
                        qualifier = q

                    ts = c.timestamp_micros
                    labels = []  # list(c.labels)
                    value = c.value

                    # merge split cells
                    if c.value_size > 0:
                        buffer = [value]
                        # throws when early eos
                        c = await it.__anext__()

                        while c.value_size > 0:
                            f = c.family_name
                            q = c.qualifier
                            t = c.timestamp_micros
                            l = c.labels
                            if f and f != family:
                                raise InvalidChunk("family changed mid cell")
                            if q and q != qualifier:
                                raise InvalidChunk("qualifier changed mid cell")
                            if t and t != ts:
                                raise InvalidChunk("timestamp changed mid cell")
                            if l and l != labels:
                                raise InvalidChunk("labels changed mid cell")

                            buffer.append(c.value)

                            # throws when premature end
                            c = await it.__anext__()

                            if c.reset_row:
                                raise _ResetRow()
                        else:
                            buffer.append(c.value)
                        value = b''.join(buffer)

                    cells.append(Cell(family, qualifier, ts, labels, value))
                    if c.commit_row:
                        yield Row(row_key, cells)
                        break
                    c = await it.__anext__()
            except _ResetRow:
                continue
    except StopAsyncIteration:
        pass

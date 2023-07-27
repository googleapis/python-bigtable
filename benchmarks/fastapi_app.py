from google.cloud.bigtable.data import BigtableDataClientAsync
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import random

app = FastAPI()

@app.on_event("startup")
async def startup_event():
    """set up client on app start"""
    global table
    client = BigtableDataClientAsync(pool_size=3)
    table = client.get_table('sanche-test', 'benchmarks')
    # from populate_table import populate_table
    # await populate_table(table)

@app.get("/", response_class=HTMLResponse)
async def root():
    return "<a href='/point_read'>point_read</a>"

@app.get("/point_read")
async def point_read():
    """read from table for each response"""
    chosen_idx = random.randint(0, 10_000)
    chosen_key = chosen_idx.to_bytes(8, byteorder="big")
    row = await table.read_row(chosen_key)
    return f"{chosen_idx}: {row if not row else row[0].value}"

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)

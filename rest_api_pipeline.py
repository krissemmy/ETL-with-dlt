import dlt
import os
from dlt.sources.rest_api import rest_api_source
from dlt.sources.helpers import requests

RPC_URL = "https://ethereum-rpc.publicnode.com"
CHAIN_NAME = "ethereum"
START_FROM_LATEST_N = 150

# ---------- JSON-RPC helpers ----------

def _rpc(method: str, params: list):
    r = requests.post(
        RPC_URL,
        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
        timeout=30,
    )
    r.raise_for_status()
    res = r.json()
    if "error" in res:
        raise RuntimeError(res["error"])
    return res["result"]

def _hex_to_int(h: str) -> int:
    return int(h, 16) if isinstance(h, str) else h

def _latest_block_number() -> int:
    return _hex_to_int(_rpc("eth_blockNumber", []))

def _get_block_by_number(num: int):
    # omit transactions for speed
    b = _rpc("eth_getBlockByNumber", [hex(num), False])
    if not b:
        return {}
    return {
        "number": _hex_to_int(b["number"]),
        "timestamp": _hex_to_int(b["timestamp"]),   # seconds since epoch
        "gas_limit": _hex_to_int(b["gasLimit"]),
        "gas_used": _hex_to_int(b["gasUsed"]),
        "size": _hex_to_int(b.get("size", "0x0")),
        "base_fee_per_gas": _hex_to_int(b.get("baseFeePerGas", "0x0")),
        "transactions_count": len(b.get("transactions", [])),
        "hash": b["hash"],
        "parent_hash": b["parentHash"],
    }

def _iter_blocks(last_n: int):
    latest = _latest_block_number()
    start = max(0, latest - last_n + 1)
    batch= list()
    for n in range(start, latest + 1):
        row = _get_block_by_number(n)
        if row:
            batch.append(row)
        if len(batch) >= 50:
            yield batch
            batch = []
    if batch:
        yield batch

# ---------- dlt resource & pipeline ----------

@dlt.resource(name="blocks", write_disposition="merge", primary_key="number")
def blocks_source(last_n: int = START_FROM_LATEST_N):
    """Yield rows (dicts) in batches."""
    for batch in _iter_blocks(last_n):
        yield batch

def load_blockchain_blocks() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="blockchain_blocks_to_postgres",
        destination="postgres",     # uses .dlt/secrets.toml
        dataset_name=CHAIN_NAME,    # schema name in Postgres
    )
    load_info = pipeline.run(blocks_source())
    print(load_info)
    print("\nTry this SQL:")
    print(f"""SELECT number AS block_number, to_timestamp(timestamp) AS block_ts, transactions_count, gas_used FROM {CHAIN_NAME}.blocks ORDER BY number DESC LIMIT 100;""")
    print("------------------------------------------------------------------------------------------------")
    print("\n")


if __name__ == "__main__":
    load_blockchain_blocks()

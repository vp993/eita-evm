#!/usr/bin/env python3
"""Fetch an Avalanche C-Chain block window (blocks, receipts, prestate, post-diff, ancestor hashes)
for Eita Avalanche PoC replay and verification."""

import argparse
import concurrent.futures
import json
import os
import urllib.request


def rpc_call(url, method, params):
    req = urllib.request.Request(
        url,
        data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        body = json.loads(resp.read())
    if "error" in body:
        raise RuntimeError(f"{method}({params}) -> {body['error']}")
    return body["result"]


def fetch_window(url, out_dir, start, end, workers=8):
    os.makedirs(out_dir, exist_ok=True)

    # Fetch 256 ancestor block hashes for the BLOCKHASH opcode window [start-256 .. start-1]
    hashes = {}
    def get_hash(n):
        b = rpc_call(url, "eth_getBlockByNumber", [hex(n), False])
        return n, b["hash"]

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(workers, 16)) as ex:
        for n, h in ex.map(get_hash, range(max(0, start - 256), start)):
            hashes[str(n)] = h

    with open(os.path.join(out_dir, "ancestor-hashes.json"), "w") as f:
        json.dump(hashes, f)

    def fetch_block(n):
        hx = hex(n)
        blk = rpc_call(url, "eth_getBlockByNumber", [hx, True])
        rec = rpc_call(url, "eth_getBlockReceipts", [hx])
        pre = rpc_call(url, "debug_traceBlockByNumber", [hx, {"tracer": "prestateTracer", "tracerConfig": {"diffMode": False}}])
        diff = rpc_call(url, "debug_traceBlockByNumber", [hx, {"tracer": "prestateTracer", "tracerConfig": {"diffMode": True}}])
        with open(os.path.join(out_dir, f"{n}.json"), "w") as f:
            json.dump({"block": blk, "receipts": rec}, f)
        with open(os.path.join(out_dir, f"{n}.meta.json"), "w") as f:
            json.dump({"receipts": rec, "prestate": pre, "diff": diff}, f)
        return n, len(blk.get("transactions", [])), bool(blk.get("settledHeight") is not None), bool(blk.get("blockExtraData", "0x") != "0x")

    total_txs = 0
    sae_count = 0
    atomic_count = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        for n, cnt, is_sae, has_atomic in ex.map(fetch_block, range(start, end + 1)):
            total_txs += cnt
            if is_sae:
                sae_count += 1
            if has_atomic:
                atomic_count += 1

    print(f"Fetched {out_dir}: blocks {start}..{end} ({end - start + 1} blocks, {total_txs} txs, sae={sae_count}, atomic={atomic_count})")


def main():
    p = argparse.ArgumentParser(description="Fetch Avalanche C-Chain replay window for Eita PoC")
    p.add_argument("--rpc", default=os.environ.get("AVAX_RPC_URL", "http://10.35.0.120:9650/ext/bc/C/rpc"))
    p.add_argument("--out", required=True)
    p.add_argument("--from", dest="start", type=int, required=True)
    p.add_argument("--to", dest="end", type=int, required=True)
    p.add_argument("--workers", type=int, default=8)
    args = p.parse_args()
    fetch_window(args.rpc, args.out, args.start, args.end, workers=args.workers)


if __name__ == "__main__":
    main()

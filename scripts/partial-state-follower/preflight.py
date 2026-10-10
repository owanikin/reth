#!/usr/bin/env python3
"""Read-only readiness check: require BAL-bearing heads and advancing persisted state."""

import argparse
import datetime
from pathlib import Path
import re
import sys
import time
import uuid

from common import DEFAULT_OUTPUT, db, endpoint, inspect, positive_int, rpc, save, unchanged


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--donor", required=True, help="Full producer container ID or name")
    parser.add_argument("--timeout", type=positive_int, default=900)
    parser.add_argument("--interval", type=positive_int, default=24)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--donor-datadir", default="/data/reth/execution-data")
    parser.add_argument("--donor-chain", default="/network-configs/genesis.json")
    args = parser.parse_args(argv)
    donor = inspect(args.donor)
    if not donor["State"]["Running"] or donor["State"]["Paused"]:
        raise RuntimeError("Producer is not running normally")
    url = endpoint(donor, 8545)
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    directory = args.output_dir.resolve() / f"preflight-{stamp}-{uuid.uuid4().hex[:6]}"
    directory.mkdir(mode=0o700, parents=True)
    save(directory / "run.json", {"donor": donor["Id"], "image_id": donor["Image"], "rpc": url})
    print(f"Evidence: {directory}\nFull producer: {donor['Id'][:12]}\nRPC: {url}", flush=True)
    samples = []
    last = None
    baseline_finish = None
    advances = 0
    deadline = time.monotonic() + args.timeout
    try:
        while time.monotonic() < deadline:
            block = rpc(url, "eth_getBlockByNumber", ["latest", False])
            head = int(block["number"], 16)
            checkpoint = db(args.donor, "StageCheckpoints", '"Finish"', args.donor_datadir, args.donor_chain)
            if not checkpoint or type(checkpoint.get("block_number")) is not int:
                raise RuntimeError("Producer has no valid persisted checkpoint")
            finish = checkpoint["block_number"]
            bal = bool(re.fullmatch(r"0x[0-9a-fA-F]{64}", block.get("blockAccessListHash") or ""))
            if not bal:
                last, baseline_finish, advances = None, None, 0
            else:
                if last is None or head < last or finish < baseline_finish:
                    baseline_finish, advances = finish, 0
                elif head > last:
                    advances += 1
                last = head
            samples.append({"head": head, "persisted": finish, "bal": bal, "advances": advances})
            save(directory / "samples.json", samples)
            print(f"head={head} persisted={finish} BAL={'yes' if bal else 'no'} advancing_samples={advances}/5", flush=True)
            if bal and advances >= 5 and finish > baseline_finish:
                if not unchanged(donor, inspect(args.donor)):
                    raise RuntimeError("Producer restarted during readiness check")
                save(directory / "result.json", {"passed": True})
                print("PASS producer preflight: BAL-bearing heads and persisted state are advancing.", flush=True)
                return
            time.sleep(args.interval)
        raise RuntimeError("Producer readiness not demonstrated; do not start the follower test")
    except BaseException as error:
        save(directory / "failure.json", {"error": str(error)})
        raise


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"FAIL: {error}", file=sys.stderr)
        sys.exit(1)

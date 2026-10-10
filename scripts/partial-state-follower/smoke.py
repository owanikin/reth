#!/usr/bin/env python3
"""Bootstrap, advance and restart a new follower without changing the donor."""

import argparse
import datetime
import json
import os
from pathlib import Path
import re
import sys
import time
import uuid

from common import (
    DEFAULT_OUTPUT, FULL_TABLES, TRACKED, ZERO, check_execution_boundary, container_logs, db, endpoint,
    inspect, positive_int, rpc, run, save, unchanged, verify_checkpoint, verify_syncing,
)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--donor", required=True, help="Running full peer container ID/name")
    parser.add_argument("--image", default="reth-partial-statefulness:local")
    parser.add_argument("--timeout", type=positive_int, default=240, help="Seconds per progress phase")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--donor-datadir", default="/data/reth/execution-data")
    parser.add_argument("--donor-chain", default="/network-configs/genesis.json")
    parser.add_argument("--tracked-contract", default=TRACKED,
                        help="Contract with nonempty code to retain and compare")
    args = parser.parse_args(argv)
    if not re.fullmatch(r"0x[0-9a-fA-F]{40}", args.tracked_contract):
        parser.error("--tracked-contract must be a 20-byte hex address")
    donor = inspect(args.donor)
    if not donor["State"]["Running"] or donor["State"]["Paused"]:
        raise RuntimeError("The full peer must already be running and unpaused")
    networks = list(donor["NetworkSettings"]["Networks"])
    if len(networks) != 1:
        raise RuntimeError(f"Expected one donor network, found {networks}")
    full_rpc = endpoint(donor, 8545)
    node = rpc(full_rpc, "admin_nodeInfo", [])
    genesis = rpc(full_rpc, "eth_getBlockByNumber", ["0x0", False])
    chain_id = int(rpc(full_rpc, "eth_chainId", []), 16)
    image = inspect(args.image)["Id"]
    if image != donor["Image"]:
        raise RuntimeError("Producer and follower must use the same image; rebuild/relaunch the producer first")
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    name = f"reth-ps-follower-{stamp.lower()}-{uuid.uuid4().hex[:6]}"
    directory = args.output_dir.resolve() / name
    if "," in str(directory):
        raise RuntimeError("Docker bind-mount evidence paths cannot contain commas")
    directory.mkdir(mode=0o700, parents=True)
    volume = name + "-data"
    secret = os.urandom(32)
    with os.fdopen(os.open(directory / "jwt.hex", os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600), "w") as jwt:
        jwt.write(secret.hex() + "\n")
    run("docker", "cp", f"{args.donor}:{args.donor_chain}", str(directory / "genesis.json"))
    mounts = ["--mount", f"type=volume,src={volume},dst=/data",
              "--mount", f"type=bind,src={directory},dst=/test,readonly"]
    details = {"schema_version": 1, "container": name, "volume": volume, "directory": str(directory),
               "donor": donor["Id"], "donor_started_at": donor["State"]["StartedAt"],
               "image_id": image, "full_rpc": full_rpc,
               "donor_datadir": args.donor_datadir, "donor_chain": args.donor_chain,
               "tracked_contract": args.tracked_contract}
    save(directory / "run.json", details)
    print(f"Evidence: {directory}", flush=True)
    created = False

    def offline_checksums(label):
        checksums = {}
        for table in FULL_TABLES:
            output = run("docker", "run", "--rm", "--network", "none", *mounts, image,
                         "--color", "never", "db", "--datadir", "/data", "--chain", "/test/genesis.json",
                         "checksum", "mdbx", table)
            (directory / f"{label}-{table}.txt").write_text(output)
            match = re.search(r"Checksum for table `[^`]+`: (0x[0-9a-f]+)", output)
            if not match:
                raise RuntimeError(f"No checksum in output for {table}")
            checksums[table] = match.group(1)
        save(directory / f"{label}-checksums.json", checksums)
        return checksums

    def logs():
        return container_logs(name)

    def checkpoint():
        return db(name, "PartialStateCheckpoints", "0", "/data", "/test/genesis.json")

    def wait_checkpoint(number, expected_hash, label, budget=None):
        deadline = time.monotonic() + (args.timeout if budget is None else budget)
        while time.monotonic() < deadline:
            if not inspect(name)["State"]["Running"]:
                raise RuntimeError("Follower exited; see follower.log")
            cp = checkpoint()
            if cp and cp["sync_complete"] and cp["block_number"] == number:
                if cp["block_hash"] != expected_hash:
                    raise RuntimeError("Checkpoint hash differs from requested target")
                header = verify_checkpoint(cp, full_rpc, directory / f"{label}-comparison.json")
                evidence = {"phase": label, "checkpoint": cp,
                            "header_root": header["stateRoot"], "match": True}
                save(directory / f"{label}.json", evidence)
                print(f"PASS {label}: block={number} root_match=true", flush=True)
                return cp
            time.sleep(2)
        raise RuntimeError(f"Timed out waiting for {label} at block {number}; inspect follower.log")

    try:
        run("docker", "run", "--rm", "--network", "none", *mounts, image,
            "init", "--chain", "/test/genesis.json", "--datadir", "/data", timeout=120)
        before = offline_checksums("before")
        run("docker", "create", "--name", name, "--network", networks[0], *mounts,
            "--label", "reth.partial-state-lifecycle=follower",
            "-p", "127.0.0.1::8545", "-p", "127.0.0.1::8551", image,
            "node", "-vvv", "--color", "never", "--datadir", "/data",
            "--chain", "/test/genesis.json", "--config", "/test/follower.toml",
            "--http", "--http.addr", "0.0.0.0", "--http.api", "eth,net,web3,admin",
            "--authrpc.addr", "0.0.0.0", "--authrpc.jwtsecret", "/test/jwt.hex",
            "--disable-discovery", "--nat", "none", "--trusted-only",
            "--trusted-peers", node["enode"])
        created = True

        for attempt in range(1, 4):
            # Start immediately after a new persisted pivot, not near the end of its slot.
            persisted = db(args.donor, "StageCheckpoints", '"Finish"',
                           args.donor_datadir, args.donor_chain)
            if not persisted:
                raise RuntimeError("No persisted full-peer checkpoint")
            previous = persisted["block_number"]
            deadline = time.monotonic() + args.timeout
            while persisted["block_number"] <= previous:
                if time.monotonic() > deadline:
                    raise RuntimeError("Full-peer persistence is not advancing")
                time.sleep(0.5)
                persisted = db(args.donor, "StageCheckpoints", '"Finish"',
                               args.donor_datadir, args.donor_chain)
                if not persisted:
                    raise RuntimeError("Full-peer checkpoint disappeared")
            seed = rpc(full_rpc, "eth_getBlockByNumber", [hex(persisted["block_number"]), False])
            if not seed or not seed.get("blockAccessListHash"):
                raise RuntimeError("The persisted full-peer pivot is not BAL-bearing yet")
            (directory / "follower.toml").write_text(
                '[partial_state]\nenabled = true\nfollower = true\n'
                f'contracts = ["{args.tracked_contract}"]\nbal_retention = 256\n\n'
                '[partial_state.trusted_checkpoint]\n'
                f'chain_id = {chain_id}\ngenesis_hash = "{genesis["hash"]}"\n'
                f'block_number = {persisted["block_number"]}\n'
                f'block_hash = "{seed["hash"]}"\nstate_root = "{seed["stateRoot"]}"\n')
            save(directory / f"seed-attempt-{attempt}.json", seed)
            save(directory / "seed.json", seed)
            run("docker", "start", name)
            deadline = time.monotonic() + 30
            while "Starting non-validating partial-state follower" not in logs():
                if time.monotonic() > deadline or not inspect(name)["State"]["Running"]:
                    raise RuntimeError("Image did not start in follower mode; check its build and follower.log")
                time.sleep(1)
            info = inspect(name)
            follower_rpc = endpoint(info, 8545)
            engine_rpc = endpoint(info, 8551)
            details.update(follower_rpc=follower_rpc, engine_rpc=engine_rpc)
            save(directory / "run.json", details)
            print(f"Bootstrap attempt {attempt}, pivot={persisted['block_number']}, RPC={follower_rpc}", flush=True)
            try:
                head = wait_checkpoint(persisted["block_number"], seed["hash"], "bootstrap", budget=30)
                break
            except RuntimeError as error:
                output = logs()
                (directory / f"bootstrap-attempt-{attempt}.log").write_text(output)
                if (attempt == 3 or "Timed out" not in str(error)
                        or "Partial-state snap sync failed" in output):
                    raise
                run("docker", "stop", "--time", "30", name)
                print("Pivot availability window missed; retrying only the new follower", flush=True)

        def advance(label):
            deadline = time.monotonic() + args.timeout
            while True:
                target = rpc(full_rpc, "eth_getBlockByNumber", ["latest", False])
                if int(target["number"], 16) > head["block_number"]:
                    break
                if time.monotonic() > deadline:
                    raise RuntimeError("The source chain stopped advancing")
                time.sleep(2)
            if int(target["number"], 16) - head["block_number"] > 128:
                raise RuntimeError("Target is too far ahead for this short smoke test")
            response = rpc(engine_rpc, "engine_forkchoiceUpdatedV3", [{
                "headBlockHash": target["hash"], "safeBlockHash": ZERO, "finalizedBlockHash": ZERO,
            }, None], secret)
            save(directory / f"{label}-engine-response.json", response)
            verify_syncing(response)
            print("PASS Engine response: SYNCING, no latestValidHash or payloadId", flush=True)
            return wait_checkpoint(int(target["number"], 16), target["hash"], label)

        peers = rpc(follower_rpc, "admin_peers", [])
        save(directory / "peers.json", peers)
        if len(peers) != 1 or peers[0]["protocols"]["eth"]["version"] != 71 or "snap/1" not in peers[0]["caps"]:
            raise RuntimeError("The follower must negotiate eth/71 and snap/1 with the donor")
        head = advance("advance-1")
        head = advance("advance-2")
        finish = db(name, "StageCheckpoints", '"Finish"', "/data", "/test/genesis.json")
        executed = rpc(follower_rpc, "eth_blockNumber", [])
        save(directory / "execution-boundary.json", {"finish": finish, "eth_blockNumber": executed})
        check_execution_boundary(finish, executed)
        code = rpc(follower_rpc, "eth_getCode", [args.tracked_contract, "latest"])
        expected = rpc(full_rpc, "eth_getCode", [args.tracked_contract, hex(head["block_number"])])
        if code != expected or code == "0x":
            raise RuntimeError("Tracked code comparison failed")
        save(directory / "tracked-code.json", {"code_bytes": (len(code) - 2) // 2, "match": True})

        run("docker", "stop", "--time", "30", name)
        after = offline_checksums("after")
        if after != before:
            raise RuntimeError("Full-state table checksums changed")
        print("PASS full-state boundary: Finish=0; four full-state checksums unchanged", flush=True)
        old_completions = logs().count("Partial-state snap sync complete")
        run("docker", "start", name)
        info = inspect(name)
        follower_rpc, engine_rpc = endpoint(info, 8545), endpoint(info, 8551)
        details.update(follower_rpc=follower_rpc, engine_rpc=engine_rpc)
        save(directory / "run.json", details)
        deadline = time.monotonic() + args.timeout
        while "Resuming persisted partial-state canonical checkpoint" not in logs().split("Starting non-validating partial-state follower")[-1]:
            if time.monotonic() > deadline or not inspect(name)["State"]["Running"]:
                raise RuntimeError("No checkpoint-resume log after restart")
            time.sleep(2)
        wait_checkpoint(head["block_number"], head["block_hash"], "restart-resume")
        if logs().count("Partial-state snap sync complete") != old_completions:
            raise RuntimeError("Restart repeated snap sync")
        head = advance("after-restart")
        finish = db(name, "StageCheckpoints", '"Finish"', "/data", "/test/genesis.json")
        check_execution_boundary(finish, rpc(follower_rpc, "eth_blockNumber", []))
        if not unchanged(donor, inspect(args.donor)):
            raise RuntimeError("Donor was restarted during the test")
        save(directory / "result.json", {
            "passed": True, "checkpoint": head, "checksums_unchanged_before_restart": True,
            "donor_not_restarted": True, "finish_after_restart": finish,
        })
        print(f"PASS smoke test. Follower left running: {name}\nEvidence: {directory}", flush=True)
    except BaseException as error:
        save(directory / "failure.json", {"error": str(error)})
        try:
            if created and inspect(name)["State"]["Running"]:
                run("docker", "stop", "--time", "30", name)
        except Exception:
            pass
        print(f"FAIL: evidence retained at {directory}. Existing devnet was not stopped.", flush=True)
        raise
    finally:
        if created:
            try:
                (directory / "follower.log").write_text(logs())
            except Exception as error:
                print(f"Could not collect follower logs: {error}", file=sys.stderr)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)

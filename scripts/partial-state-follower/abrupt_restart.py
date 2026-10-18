#!/usr/bin/env python3
"""SIGKILL a test follower at a committed checkpoint, reopen its data and advance again."""

import argparse
import json
from pathlib import Path
import re
import sys
import time
import urllib.error
import uuid

import common


def matches_bind_source(source, directory):
    if not isinstance(source, str) or not Path(source).is_absolute():
        return False
    if Path(source).resolve() == directory:
        return True
    # Docker Desktop on macOS exposes host bind paths beneath the VM's /host_mnt.
    # Translate only that exact prefix, not arbitrary paths with a matching suffix.
    if sys.platform == "darwin" and source.startswith("/host_mnt/"):
        return Path(source[len("/host_mnt"):]).resolve() == directory
    return False


def validate_follower(follower, donor, details, directory):
    if (follower["Id"] == donor["Id"]
            or follower["Name"].lstrip("/") != details["container"]
            or not details["container"].startswith("reth-ps-follower-")
            or (follower["Config"].get("Labels") or {}).get("reth.partial-state-lifecycle") != "follower"):
        raise RuntimeError("Refusing to kill a container not created as a test follower")
    mounts = {mount["Destination"]: mount for mount in follower.get("Mounts", [])}
    data, config = mounts.get("/data", {}), mounts.get("/test", {})
    if (data.get("Type") != "volume" or data.get("Name") != details["volume"]
            or config.get("Type") != "bind" or config.get("RW") is not False
            or not matches_bind_source(config.get("Source"), directory)):
        raise RuntimeError(
            "Follower mounts do not match the smoke evidence and data volume: "
            f"expected volume={details['volume']!r} and read-only /test from {str(directory)!r}; "
            f"got /data type={data.get('Type')!r} volume={data.get('Name')!r}, "
            f"/test type={config.get('Type')!r} source={config.get('Source')!r} RW={config.get('RW')!r}"
        )
    host = follower["HostConfig"]
    if host.get("AutoRemove") or host.get("RestartPolicy", {}).get("Name") not in ("", "no"):
        raise RuntimeError("Follower must disable auto-remove and automatic restarts")
    for node in (follower, donor):
        if not node["State"]["Running"] or node["State"]["Paused"]:
            raise RuntimeError(f"Node is not running normally: {node['Name']}")
        if node["Image"] != details["image_id"]:
            raise RuntimeError("Image differs from the successful smoke run")


def wait_rpc(container, chain_id, secret, timeout, directory):
    deadline = time.monotonic() + timeout
    initial = common.inspect(container)
    while True:
        info = common.inspect(container)
        if not common.unchanged(initial, info):
            raise RuntimeError("Follower exited, paused or restarted unexpectedly while opening RPC")
        ports = info["NetworkSettings"].get("Ports") or {}
        if all(ports.get(f"{port}/tcp") for port in (8545, 8551)):
            http, engine = common.endpoint(info, 8545), common.endpoint(info, 8551)
            try:
                if common.rpc(http, "eth_chainId", []) != chain_id:
                    raise RuntimeError("Follower RPC reports the wrong chain ID")
                capabilities = common.rpc(engine, "engine_exchangeCapabilities", [[]], secret)
                if not isinstance(capabilities, list) or "engine_forkchoiceUpdatedV3" not in capabilities:
                    raise RuntimeError("Follower lacks engine_forkchoiceUpdatedV3")
            except urllib.error.HTTPError:
                raise
            except (urllib.error.URLError, ConnectionError, TimeoutError):
                pass
            else:
                common.save(directory / "restarted-rpc.json", {"http": http, "engine": engine})
                return info, http, engine
        if time.monotonic() >= deadline:
            raise RuntimeError("Timed out waiting for follower HTTP/Engine RPC after SIGKILL")
        time.sleep(1)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", required=True, help="Successful smoke --call-data evidence directory")
    parser.add_argument("--timeout", type=common.positive_int, default=240, help="Seconds per progress phase")
    parser.add_argument("--allow-abrupt-stop", action="store_true",
                        help="Allow SIGKILL of only the smoke-test follower, keeping its data volume")
    args = parser.parse_args(argv)
    if not args.allow_abrupt_stop:
        parser.error("this test requires --allow-abrupt-stop")
    if not args.run.strip():
        parser.error("--run is empty; set RUN to the Evidence directory from a successful smoke --call-data run")
    args.run = Path(args.run).resolve()
    required = ("run.json", "result.json", "before-checksums.json", "jwt.hex")
    missing = [name for name in required if not (args.run / name).is_file()]
    if missing:
        parser.error(f"--run is not a complete smoke evidence directory: {args.run}; "
                     f"missing {', '.join(missing)}. Use the directory printed by a successful "
                     "smoke --call-data run, not the repository or an outage/RPC subdirectory")
    details = json.loads((args.run / "run.json").read_text())
    result = json.loads((args.run / "result.json").read_text())
    if (details.get("schema_version") != 1 or result.get("passed") is not True
            or result.get("checkpoint_calls_checked") is not True):
        raise RuntimeError("Use a successful smoke --call-data run before abrupt restart testing")
    tracked, calldata = details["tracked_contract"], details.get("call_data")
    if (not re.fullmatch(r"0x[0-9a-fA-F]{40}", tracked)
            or not isinstance(calldata, str) or not re.fullmatch(r"0x(?:[0-9a-fA-F]{2})*", calldata)):
        raise RuntimeError("Invalid tracked contract or call_data in smoke evidence")
    genesis = json.loads((args.run / "before-checksums.json").read_text())
    if (set(genesis) != set(common.FULL_TABLES)
            or any(not isinstance(value, str) or not re.fullmatch(r"0x[0-9a-fA-F]+", value)
                   for value in genesis.values())):
        raise RuntimeError("Missing full-state genesis checksums from smoke")
    secret = bytes.fromhex((args.run / "jwt.hex").read_text().strip())
    if len(secret) != 32:
        raise RuntimeError("Invalid smoke Engine JWT secret")
    follower, donor = common.inspect(details["container"]), common.inspect(details["donor"])
    validate_follower(follower, donor, details, args.run)
    container = follower["Id"]
    http, engine = common.endpoint(follower, 8545), common.endpoint(follower, 8551)
    producer = common.endpoint(donor, 8545)
    chain_id = common.rpc(producer, "eth_chainId", [])
    if common.rpc(http, "eth_chainId", []) != chain_id:
        raise RuntimeError("Follower and producer chain IDs differ")
    directory = args.run / ("abrupt-restart-" + uuid.uuid4().hex[:10])
    directory.mkdir(mode=0o700)
    common.save(directory / "run.json", {
        "follower": container, "donor": donor["Id"], "image_id": details["image_id"],
        "volume": details["volume"], "signal": "SIGKILL", "scope": "after-committed-checkpoint",
    })
    common.save(directory / "follower-before.json", follower)
    common.save(directory / "donor-before.json", donor)
    print(f"Evidence: {directory}", flush=True)
    kill_attempted = restart_attempted = False

    def nodes_unchanged():
        for node in (follower, donor):
            if not common.unchanged(node, common.inspect(node["Id"])):
                raise RuntimeError("A tested node unexpectedly restarted, stopped or paused")

    def checkpoint():
        cp = common.db(container, "PartialStateCheckpoints", "0", "/data", "/test/genesis.json")
        if not cp or cp.get("sync_complete") is not True:
            raise RuntimeError("Missing or incomplete verified follower checkpoint")
        return cp

    def stable(cp):
        nodes_unchanged()
        if checkpoint() != cp:
            raise RuntimeError("Checkpoint changed unexpectedly; run without concurrent Engine targets")

    def boundary():
        finish = common.db(container, "StageCheckpoints", '"Finish"', "/data", "/test/genesis.json")
        common.check_execution_boundary(finish, common.rpc(http, "eth_blockNumber", []))
        return finish

    def verify(cp, label):
        common.verify_checkpoint(cp, producer, directory / f"{label}-comparison.json")

    def send_target(target, label):
        response = common.rpc(engine, "engine_forkchoiceUpdatedV3", [{
            "headBlockHash": target["hash"], "safeBlockHash": common.ZERO,
            "finalizedBlockHash": common.ZERO,
        }, None], secret)
        common.save(directory / f"{label}-engine.json", response)
        common.verify_syncing(response)

    def wait_checkpoint(target, label):
        deadline = time.monotonic() + args.timeout
        while time.monotonic() < deadline:
            nodes_unchanged()
            cp = checkpoint()
            if cp["block_hash"] == target["hash"]:
                verify(cp, label)
                common.save(directory / f"{label}-checkpoint.json", cp)
                return cp
            time.sleep(2)
        raise RuntimeError(f"Timed out waiting for {label}; see follower.log")

    def checksums(cp, tables, label):
        stable(cp)
        values = common.table_checksums(container, tables, directory, label)
        stable(cp)
        if any(values[table] != genesis[table] for table in common.FULL_TABLES):
            raise RuntimeError("Full-state table checksums differ from the genesis baseline")
        return values

    def queries(cp, label):
        stable(cp)
        at = {"blockHash": cp["block_hash"]}
        call = {"to": tracked, "data": calldata, "gasPrice": "0x0"}
        values, observations = {}, []
        for method, params in [
            ("eth_getCode", [tracked, at]), ("eth_getStorageAt", [tracked, "0x0", at]),
            ("eth_call", [call, at]), ("eth_estimateGas", [call, at]),
        ]:
            actual = common.rpc(http, method, params)
            expected = common.rpc(producer, method, params)
            observations.append({"method": method, "params": params, "follower": actual,
                                 "producer": expected, "match": actual == expected})
            common.save(directory / f"{label}-rpc.json", observations)
            if actual != expected:
                raise RuntimeError(f"Checkpoint RPC comparison failed: {method}")
            if (not isinstance(actual, str) or not re.fullmatch(r"0x[0-9a-fA-F]*", actual)
                    or (method == "eth_getCode" and actual == "0x")
                    or (method == "eth_getStorageAt" and len(actual) != 66)
                    or (method == "eth_estimateGas" and (actual == "0x" or int(actual, 16) < 21000))
                    or (method in ("eth_call", "eth_getCode") and len(actual) % 2)):
                raise RuntimeError(f"Malformed {method} result")
            values[method] = actual
            stable(cp)
        return values

    def restart_logs(since):
        output = common.container_logs(container, since)
        (directory / "restart.log").write_text(output)
        if "Starting partial-state snap sync" in output or "Partial-state snap sync complete" in output:
            raise RuntimeError("Restart repeated snap bootstrap instead of resuming")
        return output

    try:
        previous = checkpoint()
        verify(previous, "initial")
        target = common.rpc(producer, "eth_getBlockByNumber", ["latest", False])
        if not 0 <= int(target["number"], 16) - previous["block_number"] <= 128:
            raise RuntimeError("Follower is too far behind for this bounded test; use a fresh smoke run")
        send_target(target, "catch-up")
        baseline = wait_checkpoint(target, "before-kill")
        before_rpc = queries(baseline, "before-kill")
        before = checksums(baseline, common.STATE_TABLES, "before-kill")
        boundary()
        stable(baseline)
        # The checkpoint is stationary: no target is outstanding when SIGKILL is sent.
        validate_follower(common.inspect(container), common.inspect(donor["Id"]), details, args.run)
        kill_attempted = True
        common.run("docker", "kill", "--signal", "SIGKILL", container, timeout=30)
        deadline = time.monotonic() + args.timeout
        while True:
            killed = common.inspect(container)
            common.save(directory / "follower-killed.json", killed)
            state = killed["State"]
            if not state["Running"] or time.monotonic() >= deadline:
                break
            time.sleep(1)
        if (state["Running"] or state["Paused"] or state.get("OOMKilled") is not False
                or state.get("ExitCode") != 137 or not state.get("FinishedAt")
                or state["StartedAt"] != follower["State"]["StartedAt"]
                or killed["RestartCount"] != follower["RestartCount"]):
            raise RuntimeError("SIGKILL termination was not confirmed (expected stopped, exit 137, not OOM)")
        print(f"PASS abrupt stop: block={baseline['block_number']}; SIGKILL exit=137", flush=True)
        restart_attempted = True
        common.run("docker", "start", container)
        restarted, http, engine = wait_rpc(container, chain_id, secret, args.timeout, directory)
        validate_follower(restarted, common.inspect(donor["Id"]), details, args.run)
        if (restarted["State"]["StartedAt"] == follower["State"]["StartedAt"]
                or restarted["RestartCount"] != follower["RestartCount"]):
            raise RuntimeError("Expected one manual follower restart, not an automatic restart")
        follower = restarted
        common.save(directory / "follower-restarted.json", follower)
        deadline = time.monotonic() + args.timeout
        while True:
            nodes_unchanged()
            output = restart_logs(state["FinishedAt"])
            if "Resuming persisted partial-state canonical checkpoint" in output:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("No verified checkpoint-resume log after SIGKILL")
            time.sleep(1)
        recovered = checkpoint()
        if recovered != baseline:
            raise RuntimeError("Restart did not preserve the entire committed checkpoint")
        verify(recovered, "recovered")
        common.save(directory / "recovered-checkpoint.json", recovered)
        if queries(recovered, "recovered") != before_rpc:
            raise RuntimeError("Recovered RPC state differs from pre-kill state")
        if checksums(recovered, common.STATE_TABLES, "recovered") != before:
            raise RuntimeError("State or journal table checksums changed across abrupt restart")
        boundary()
        print(f"PASS recovered checkpoint: block={recovered['block_number']}; RPC and {len(common.STATE_TABLES)} table checksums unchanged", flush=True)
        deadline = time.monotonic() + args.timeout
        while True:
            nodes_unchanged()
            target = common.rpc(producer, "eth_getBlockByNumber", ["latest", False])
            gap = int(target["number"], 16) - recovered["block_number"]
            if gap > 128 or gap < 0:
                raise RuntimeError("Producer target is outside this bounded recovery scenario")
            if gap > 0:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("Producer stopped advancing; recovery test is inconclusive")
            time.sleep(2)
        send_target(target, "continued")
        final = wait_checkpoint(target, "continued")
        queries(final, "continued")
        checksums(final, common.FULL_TABLES, "continued")
        finish = boundary()
        stable(final)
        restart_logs(state["FinishedAt"])
        common.save(directory / "donor-after.json", common.inspect(donor["Id"]))
        common.save(directory / "result.json", {
            "passed": True, "scope": "SIGKILL-after-committed-checkpoint", "baseline": baseline,
            "recovered": recovered, "final": final, "tables_checked": list(common.STATE_TABLES),
            "state_and_journals_unchanged_on_reopen": True, "checkpoint_rpc_matches": True,
            "no_repeat_snap": True, "donor_not_restarted": True, "finish": finish,
            "full_state_checksums_match_genesis": True,
        })
        print(f"PASS abrupt restart: resumed block={recovered['block_number']}, advanced to block={final['block_number']}; matching roots, SYNCING, Finish=0", flush=True)
    except BaseException as error:
        common.save(directory / "failure.json", {"error": str(error), "type": type(error).__name__})
        raise
    finally:
        # An interruption between kill and start must not silently leave the follower down.
        # A failed start is not retried repeatedly; preserve evidence for diagnosis.
        if kill_attempted and not restart_attempted:
            try:
                if not common.inspect(container)["State"]["Running"]:
                    common.run("docker", "start", container)
                    common.save(directory / "cleanup.json", {"start_requested": True})
            except Exception as error:
                common.save(directory / "cleanup.json", {"error": str(error)})
                print(f"Could not restart test follower {container}: {error}", file=sys.stderr)
        try:
            common.save(directory / "follower-final.json", common.inspect(container))
            (directory / "follower.log").write_text(common.container_logs(container))
        except Exception as error:
            print(f"Could not collect follower logs: {error}", file=sys.stderr)
        print(f"Evidence retained: {directory}. Producer was not stopped; follower data was not deleted.", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"FAIL: {error}", file=sys.stderr)
        sys.exit(1)

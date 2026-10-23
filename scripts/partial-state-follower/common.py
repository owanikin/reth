"""Shared, standard-library helpers for the Docker follower lifecycle tests."""

import argparse
import base64
import hashlib
import hmac
import json
from pathlib import Path
import re
import subprocess
import sys
import time
import urllib.request


ZERO = "0x" + "00" * 32
TRACKED = "0x00000000219ab540356cBB839Cbe05303d7705Fa"
FULL_TABLES = ("PlainAccountState", "PlainStorageState", "HashedAccounts", "HashedStorages")
STATE_TABLES = (*FULL_TABLES, "Bytecodes", "PartialStateAccounts",
                "PartialStateStorageRoots", "PartialStateStorages", "PartialStateCheckpoints",
                "PartialStateHeaders", "PartialStateTransitionJournals",
                "PartialStateAccountChangeSets", "PartialStateStorageChangeSets",
                "PartialStateAvailability", "PartialStateAvailabilityChangeSets", "StageCheckpoints")
DEFAULT_OUTPUT = Path(__file__).resolve().parents[2] / "target" / "partial-state-follower"


def positive_int(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return number


class CommandError(RuntimeError):
    def __init__(self, args, result):
        self.stderr = result.stderr
        super().__init__(f"Command failed: {' '.join(args)}\n{result.stdout}\n{result.stderr}")


def run(*args, timeout=90):
    result = subprocess.run(args, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        raise CommandError(args, result)
    return result.stdout


def inspect(container):
    results = json.loads(run("docker", "inspect", container))
    if len(results) != 1:
        raise RuntimeError(f"Expected exactly one Docker object: {container}")
    return results[0]


def endpoint(info, port):
    bindings = info["NetworkSettings"]["Ports"].get(f"{port}/tcp") or []
    if not bindings:
        raise RuntimeError(f"No published RPC port {port} on {info['Name']}")
    binding = bindings[0]
    if binding["HostIp"] not in ("", "0.0.0.0", "127.0.0.1"):
        raise RuntimeError("These tests require a local Docker daemon with IPv4 RPC bindings")
    return "http://127.0.0.1:" + binding["HostPort"]


def rpc_response(url, method, params, secret=None):
    """Return a validated JSON-RPC envelope, including expected application errors."""
    headers = {"Content-Type": "application/json"}
    if secret:
        def b64(data):
            return base64.urlsafe_b64encode(data).rstrip(b"=")
        message = b".".join((b64(b'{"alg":"HS256","typ":"JWT"}'),
                              b64(json.dumps({"iat": int(time.time())}).encode())))
        signature = b64(hmac.new(secret, message, hashlib.sha256).digest())
        headers["Authorization"] = "Bearer " + (message + b"." + signature).decode()
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    request = urllib.request.Request(url, data=body, headers=headers)
    with urllib.request.urlopen(request, timeout=15) as response:
        result = json.load(response)
    if (not isinstance(result, dict) or result.get("jsonrpc") != "2.0"
            or type(result.get("id")) is not int or result["id"] != 1
            or ("result" in result) == ("error" in result)):
        raise RuntimeError(f"Malformed JSON-RPC response for {method}: {result}")
    if "error" in result:
        error = result["error"]
        if (not isinstance(error, dict) or type(error.get("code")) is not int
                or not isinstance(error.get("message"), str)):
            raise RuntimeError(f"Malformed JSON-RPC error for {method}: {result}")
    return result


def rpc(url, method, params, secret=None):
    result = rpc_response(url, method, params, secret)
    if result.get("error") is not None:
        raise RuntimeError(f"{method}: {result['error']}")
    return result["result"]


def db(container, table, key, datadir, chain):
    # The read-only CLI checks static files before reading MDBX. A live writer
    # can temporarily change their lengths or commit them ahead of MDBX. The
    # latter can trigger a repair attempt that the read-only writer guard rejects.
    retryable_errors = {
        "Error: File is in an inconsistent state.",
        "Error: cannot get a writer on a read-only environment.",
    }
    for attempt in range(1, 6):
        try:
            output = run("docker", "exec", container, "reth", "--quiet", "db",
                         "--datadir", datadir, "--chain", chain, "get", "mdbx", table, key,
                         timeout=15)
        except CommandError as error:
            if retryable_errors.isdisjoint(error.stderr.splitlines()):
                raise
            if attempt == 5:
                raise RuntimeError(
                    f"Live DB read still fails after {attempt} attempts ({container}, {table}); "
                    "no checkpoint was accepted. Inspect the producer before retrying.\n"
                    f"{error}"
                ) from error
            print(f"Live DB consistency check failed ({table}, attempt {attempt}/5); "
                  "retrying the read-only query in 2s", file=sys.stderr, flush=True)
            time.sleep(2)
            continue
        return json.loads(output) if output.strip() else None


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def table_checksums(container, tables, directory, label):
    """Read tables on a stationary follower; callers must check checkpoint stability."""
    values = {}
    for table in tables:
        output = run("docker", "exec", container, "reth", "--color", "never",
                     "db", "--datadir", "/data", "--chain", "/test/genesis.json",
                     "checksum", "mdbx", table)
        (directory / f"{label}-{table}.txt").write_text(output)
        matches = re.findall(rf"Checksum for table `{re.escape(table)}`: (0x[0-9a-fA-F]+)", output)
        if len(matches) != 1:
            raise RuntimeError(f"Missing or ambiguous checksum for {table}")
        values[table] = matches[0]
        save(directory / f"{label}-checksums.json", values)
    return values


def container_logs(container, since=None):
    command = ["docker", "logs"]
    if since:
        command += ["--since", since]
    command.append(container)
    result = subprocess.run(command, capture_output=True, text=True, timeout=30)
    if result.returncode:
        raise CommandError(command, result)
    return re.sub(r"\x1b\[[0-9;]*m", "", result.stdout + result.stderr)


def unchanged(before, after):
    return (
        before["Id"] == after["Id"]
        and before["State"]["StartedAt"] == after["State"]["StartedAt"]
        and before["RestartCount"] == after["RestartCount"]
        and after["State"]["Running"]
        and not after["State"]["Paused"]
    )


def verify_syncing(response):
    status = response["payloadStatus"]
    if (status["status"] != "SYNCING" or status.get("latestValidHash") is not None
            or response.get("payloadId") is not None):
        raise RuntimeError(f"Unexpected follower Engine response: {response}")


def verify_checkpoint(cp, full_rpc, evidence_path=None):
    if not cp or cp.get("sync_complete") is not True:
        raise RuntimeError("No complete verified follower checkpoint")
    header = rpc(full_rpc, "eth_getBlockByHash", [cp["block_hash"], False])
    canonical = rpc(full_rpc, "eth_getBlockByNumber", [hex(cp["block_number"]), False])
    comparison = {
        "checked_at_unix": time.time(), "checkpoint": cp, "by_hash": header,
        "by_number": canonical, "match": False,
    }
    # Keep the observations even if a malformed response fails validation below.
    if evidence_path:
        save(evidence_path, comparison)
    mismatches = []
    for label, observed in (("by_hash", header), ("by_number", canonical)):
        if observed is None:
            mismatches.append(f"{label}: header unavailable (null)")
            continue
        if observed["hash"] != cp["block_hash"]:
            mismatches.append(f"{label}: block hash differs")
        if int(observed["number"], 16) != cp["block_number"]:
            mismatches.append(f"{label}: block number differs")
        if observed["stateRoot"] != cp["state_root"]:
            mismatches.append(f"{label}: state root differs")
    comparison.update(match=not mismatches, mismatches=mismatches)
    if evidence_path:
        save(evidence_path, comparison)
    if mismatches:
        location = f"; evidence: {evidence_path}" if evidence_path else ""
        raise RuntimeError(
            f"Checkpoint does not match the producer's canonical header at block "
            f"{cp['block_number']}: {'; '.join(mismatches)}{location}"
        )
    return header


def check_execution_boundary(finish, executed):
    if not finish or finish["block_number"] != 0 or executed != "0x0":
        raise RuntimeError("Normal executed/persisted head advanced beyond genesis")

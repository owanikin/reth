#!/usr/bin/env python3
"""Read-only RPC boundary checks on a successful smoke-test follower; no restarts or FCUs."""

import argparse
import json
from pathlib import Path
import re
import sys
import uuid

import common


UNTRACKED = "0x000f3df6d732807ef1319fb7b8bb8522d0beac02"
EMPTY = "0x00000000000000000000000000000000feedbeef"
EMPTY_ROOT = "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"
STATE_TABLES = (*common.FULL_TABLES, "Bytecodes", "PartialStateAccounts",
                "PartialStateStorageRoots", "PartialStateStorages", "PartialStateCheckpoints",
                "PartialStateHeaders", "PartialStateTransitionJournals",
                "PartialStateAccountChangeSets", "PartialStateStorageChangeSets", "StageCheckpoints")
WORD = re.compile(r"0x[0-9a-fA-F]{64}")
DATA = re.compile(r"0x(?:[0-9a-fA-F]{2})*")


def address(value):
    if not re.fullmatch(r"0x[0-9a-fA-F]{40}", value):
        raise argparse.ArgumentTypeError("expected a 20-byte hex address")
    return value.lower()


def storage_slot(value):
    if not re.fullmatch(r"0x[0-9a-fA-F]{1,64}", value):
        raise argparse.ArgumentTypeError("expected a hex storage slot of at most 32 bytes")
    return "0x" + f"{int(value, 16):064x}"


def sload(slot):
    # PUSH32 slot; SLOAD; MSTORE(0); RETURN(0, 32).
    return "0x7f" + slot[2:] + "5460005260206000f3"


def nested_call(target, value):
    # Write slot 0 first, then CALL the dependency and discard its success flag.
    # A missing-state database error must still abort the entire simulation.
    return ("0x7f" + value[2:] + "6000556000600060006000600073" + target[2:]
            + "5af150602a60005260206000f3")


def require_result(response, label):
    if "error" in response:
        raise RuntimeError(f"{label}: unexpected RPC error {response['error']}")
    return response["result"]


def require_missing(response, code, target, label):
    kind = "storage" if code == -32001 else "bytecode"
    expected = f"{kind} for address {target} is not tracked by this partial-state node"
    error = response.get("error") or {}
    if ("result" in response or error.get("code") != code
            or error.get("message", "").lower() != expected.lower()):
        raise RuntimeError(f"{label}: expected {code} for {target}, got {response}")


def validate_evm_result(method, value, label):
    if method == "eth_estimateGas" and (not isinstance(value, str)
            or not re.fullmatch(r"0x[0-9a-fA-F]+", value) or int(value, 16) < 21000):
        raise RuntimeError(f"{label}: invalid gas estimate")
    if method == "eth_call" and (not isinstance(value, str) or not DATA.fullmatch(value)):
        raise RuntimeError(f"{label}: invalid call result")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", required=True, help="Successful smoke --call-data evidence directory")
    parser.add_argument("--untracked-contract", type=address, default=UNTRACKED,
                        help="Existing contract with nonempty code and storage on the producer")
    parser.add_argument("--untracked-slot", type=storage_slot, default=storage_slot("0x0"))
    parser.add_argument("--empty-address", type=address, default=EMPTY,
                        help="Non-precompile address with known-empty code and storage")
    args = parser.parse_args(argv)
    if not args.run.strip():
        parser.error("--run is empty; use the Evidence directory from a successful smoke --call-data run")
    args.run = Path(args.run)
    if not all((args.run / name).is_file() for name in ("run.json", "result.json")):
        parser.error("--run must contain run.json and result.json from a successful smoke run; failed or incomplete runs cannot be used")
    details = json.loads((args.run / "run.json").read_text())
    result = json.loads((args.run / "result.json").read_text())
    if (details.get("schema_version") != 1 or result.get("passed") is not True
            or result.get("checkpoint_calls_checked") is not True):
        raise RuntimeError("Use a successful smoke --call-data run before testing RPC boundaries")
    tracked = address(details["tracked_contract"])
    calldata = details.get("call_data")
    if not isinstance(calldata, str) or not DATA.fullmatch(calldata):
        raise RuntimeError("Smoke run has no valid call_data")
    if len({tracked, args.untracked_contract, args.empty_address}) != 3:
        raise RuntimeError("Tracked, untracked and empty addresses must be distinct")
    if int(args.empty_address, 16) <= 0xffff:
        raise RuntimeError("Choose an empty address outside the reserved low-address range")
    follower, donor = common.inspect(details["container"]), common.inspect(details["donor"])
    if (follower["Id"] == donor["Id"]
            or (follower["Config"].get("Labels") or {}).get("reth.partial-state-lifecycle") != "follower"):
        raise RuntimeError("Refusing a container not created as a test follower")
    for node in (follower, donor):
        if not node["State"]["Running"] or node["State"]["Paused"]:
            raise RuntimeError(f"Node is not running normally: {node['Name']}")
        if node["Image"] != details["image_id"]:
            raise RuntimeError("Image differs from the successful smoke run")
    follower_rpc, producer_rpc = common.endpoint(follower, 8545), common.endpoint(donor, 8545)
    directory = args.run.resolve() / ("rpc-boundaries-" + uuid.uuid4().hex[:10])
    directory.mkdir(mode=0o700)
    common.save(directory / "run.json", {
        "follower": follower["Id"], "donor": donor["Id"], "image_id": follower["Image"],
        "follower_rpc": follower_rpc, "producer_rpc": producer_rpc,
        "tracked": tracked, "untracked": args.untracked_contract, "slot": args.untracked_slot,
        "empty": args.empty_address, "call_data": calldata,
    })
    print(f"Evidence: {directory}", flush=True)
    observations = []

    def request(peer, method, params, label):
        observation = {"label": label, "peer": peer, "method": method, "params": params}
        observations.append(observation)
        try:
            response = common.rpc_response(producer_rpc if peer == "producer" else follower_rpc,
                                           method, params)
            observation["response"] = response
            return response
        except Exception as error:
            observation["transport_or_protocol_error"] = str(error)
            raise
        finally:
            common.save(directory / "rpc-observations.json", observations)

    def checkpoint():
        return common.db(follower["Id"], "PartialStateCheckpoints", "0", "/data", "/test/genesis.json")

    def stable():
        if checkpoint() != baseline:
            raise RuntimeError("Checkpoint changed during RPC boundary test; do not send concurrent targets")
        for node in (follower, donor):
            if not common.unchanged(node, common.inspect(node["Id"])):
                raise RuntimeError("A tested node restarted, stopped or paused")

    def checksums(label):
        values = {}
        for table in STATE_TABLES:
            output = common.run("docker", "exec", follower["Id"], "reth", "--color", "never",
                                "db", "--datadir", "/data", "--chain", "/test/genesis.json",
                                "checksum", "mdbx", table)
            (directory / f"{label}-{table}.txt").write_text(output)
            matches = re.findall(rf"Checksum for table `{table}`: (0x[0-9a-fA-F]+)", output)
            if len(matches) != 1:
                raise RuntimeError(f"Missing or ambiguous checksum for {table}")
            values[table] = matches[0]
            common.save(directory / f"{label}-checksums.json", values)
        stable()
        return values

    def positive(method, params, label, expected=None):
        control = require_result(request("producer", method, params, label), label + " producer")
        actual = require_result(request("follower", method, params, label), label + " follower")
        if actual != control or (expected is not None and actual != expected):
            raise RuntimeError(f"{label}: result does not match producer/fixture")
        validate_evm_result(method, actual, label)
        stable()
        return actual

    try:
        baseline = checkpoint()
        common.verify_checkpoint(baseline, producer_rpc, directory / "before-comparison.json")
        common.save(directory / "checkpoint-before.json", baseline)
        at = {"blockHash": baseline["block_hash"]}
        slot, untracked = args.untracked_slot, args.untracked_contract
        tracked_request = {"to": tracked, "data": calldata, "gasPrice": "0x0"}
        untracked_request = {"to": untracked, "gasPrice": "0x0"}

        # Check fixture prerequisites on the producer, not by treating an empty response as missing.
        for target, empty in [(untracked, False), (args.empty_address, True)]:
            code = require_result(request("producer", "eth_getCode", [target, at], "fixture-code"), "fixture-code")
            response = request("producer", "eth_getProof", [target, [], at], "fixture-storage")
            if response.get("error") == {
                "code": -32602, "message": "distance to target block exceeds maximum proof window",
            }:
                raise RuntimeError(
                    f"Producer refused eth_getProof at checkpoint {baseline['block_number']}: "
                    "historical proof window exceeded (Reth defaults to 0 blocks). "
                    "Start the test producer with --rpc.eth-proof-window=4096 and run a fresh "
                    "smoke test before RPC boundaries. This flag needs no image rebuild; "
                    "editing YAML does not update a running producer. If already configured, "
                    "use a newer smoke checkpoint within that window. "
                    "No missing-state checks passed; no fallback to latest was attempted."
                )
            proof = require_result(response, "fixture-storage")
            root = proof.get("storageHash") if isinstance(proof, dict) else None
            if (not isinstance(code, str) or not DATA.fullmatch(code)
                    or not isinstance(root, str) or not WORD.fullmatch(root)
                    or (code == "0x") != empty or (root.lower() == EMPTY_ROOT) != empty):
                raise RuntimeError(f"Invalid {'empty' if empty else 'untracked'} fixture at {target}")
        before = checksums("before")
        untracked_storage = require_result(request("producer", "eth_getStorageAt", [untracked, slot, at],
                                                   "fixture-slot"), "fixture-slot")
        if not isinstance(untracked_storage, str) or not WORD.fullmatch(untracked_storage):
            raise RuntimeError("Untracked fixture storage is not a 32-byte word")
        original_storage = positive("eth_getStorageAt", [tracked, "0x0", at], "tracked-storage-before")
        if not isinstance(original_storage, str) or not WORD.fullmatch(original_storage):
            raise RuntimeError("Tracked storage is not a 32-byte word")
        original_code = positive("eth_getCode", [tracked, at], "tracked-code-before")
        if not isinstance(original_code, str) or not DATA.fullmatch(original_code) or original_code == "0x":
            raise RuntimeError("Tracked contract has no code")
        normal = {method: positive(method, [tracked_request, at], "tracked-before")
                  for method in ("eth_call", "eth_estimateGas")}
        # Change a real tracked slot in the simulation, even if its original value is zero.
        value = "0x" + f"{int(original_storage, 16) ^ 0xff:064x}"
        nested = nested_call(untracked, value)
        for method, params, code in [
            ("eth_getCode", [untracked, at], -32002),
            ("eth_getStorageAt", [untracked, slot, at], -32001),
        ]:
            require_missing(request("follower", method, params, "direct-read"), code, untracked, method)
            stable()
        for method in ("eth_call", "eth_estimateGas"):
            # Direct calls need not succeed on the producer; its code existence was checked above.
            require_missing(request("follower", method, [untracked_request, at], "direct-code"),
                            -32002, untracked, method + " direct-code")
            stable()
            cases = [
                # Loading an untracked contract account fails before code overrides are applied.
                ("untracked-code-override", untracked_request, {untracked: {"code": sload(slot)}}, -32002),
                ("nested-code-after-write", tracked_request, {tracked: {"code": nested}}, -32002),
            ]
            for label, call, overrides, code in cases:
                params = [call, at, overrides]
                control = require_result(request("producer", method, params, label), label + " control")
                validate_evm_result(method, control, label)
                if method == "eth_call" and label == "untracked-code-override" and control != untracked_storage:
                    raise RuntimeError("Full producer did not execute the SLOAD fixture")
                if method == "eth_call" and label.startswith("nested") and control != "0x" + f"{42:064x}":
                    raise RuntimeError(f"{label}: full producer did not execute the nested fixture")
                require_missing(request("follower", method, params, label), code, untracked, label)
                stable()
        print("PASS missing-state errors: untracked reads, direct/nested calls and estimates, code override rejected", flush=True)

        for method in ("eth_call", "eth_estimateGas"):
            for label, overrides in [
                ("sstore-overlay", {tracked: {"code": "0x7f" + value[2:] + "600055" + sload(common.ZERO)[2:]}}),
                ("state-diff-overlay", {tracked: {"code": sload(common.ZERO), "stateDiff": {common.ZERO: value}}}),
            ]:
                positive(method, [tracked_request, at, overrides], label, value if method == "eth_call" else None)
        for method, params, expected in [
            ("eth_getCode", [args.empty_address, at], "0x"),
            ("eth_getStorageAt", [args.empty_address, "0x0", at], common.ZERO),
            ("eth_call", [{"to": args.empty_address, "gasPrice": "0x0"}, at], "0x"),
            ("eth_estimateGas", [{"to": args.empty_address, "gasPrice": "0x0"}, at], None),
        ]:
            positive(method, params, "known-empty", expected)
        print("PASS overlays and known-empty state: successful simulations match the producer", flush=True)
        positive("eth_getStorageAt", [tracked, "0x0", at], "tracked-storage-after", original_storage)
        positive("eth_getCode", [tracked, at], "tracked-code-after", original_code)
        for method, expected in normal.items():
            positive(method, [tracked_request, at], "tracked-after", expected)
        after = checksums("after")
        common.save(directory / "checkpoint-after.json", checkpoint())
        if after != before:
            raise RuntimeError("Persisted table checksums changed during RPC simulations")
        common.verify_checkpoint(baseline, producer_rpc, directory / "after-comparison.json")
        finish = common.db(follower["Id"], "StageCheckpoints", '"Finish"', "/data", "/test/genesis.json")
        common.check_execution_boundary(finish, common.rpc(follower_rpc, "eth_blockNumber", []))
        stable()
        common.save(directory / "result.json", {
            "passed": True, "checkpoint": baseline, "missing_state_errors_checked": True,
            "tracked_calls_still_work": True, "known_empty_checked": True, "overrides_checked": True,
            "checksums_unchanged": True, "tables_checked": list(STATE_TABLES),
            "nodes_not_restarted": True, "finish": finish,
        })
        print(f"PASS RPC boundaries: block={baseline['block_number']}; checkpoint and {len(STATE_TABLES)} table checksums unchanged; no restarts", flush=True)
    except BaseException as error:
        common.save(directory / "failure.json", {"error": str(error)})
        raise
    finally:
        print(f"Evidence retained: {directory}", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"FAIL: {error}", file=sys.stderr)
        sys.exit(1)

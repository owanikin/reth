#!/usr/bin/env python3
"""Temporarily isolate an existing follower's peer traffic; never restart either node."""

import argparse
import datetime
import json
from pathlib import Path
import re
import sys
import time
import uuid


import common

# The independent timer restores connectivity even if this Python process disappears.
FIREWALL = r'''
set -eu
cleanup() {
    set +e
    iptables -w 5 -nvxL "$CHAIN"
    iptables -w 5 -D INPUT -s "$PEER" -p tcp -m multiport --ports 30303 -j "$CHAIN"
    iptables -w 5 -D OUTPUT -d "$PEER" -p tcp -m multiport --ports 30303 -j "$CHAIN"
    iptables -w 5 -F "$CHAIN"
    if iptables -w 5 -X "$CHAIN"; then
        date -u '+RESTORED=%Y-%m-%dT%H:%M:%SZ'
    else
        echo 'RESTORE FAILED: inspect firewall rules'
    fi
}
iptables -w 5 -N "$CHAIN"
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
iptables -w 5 -A "$CHAIN" -j DROP
iptables -w 5 -I INPUT 1 -s "$PEER" -p tcp -m multiport --ports 30303 -j "$CHAIN"
iptables -w 5 -I OUTPUT 1 -d "$PEER" -p tcp -m multiport --ports 30303 -j "$CHAIN"
date -u '+BLOCKED=%Y-%m-%dT%H:%M:%SZ'
sleep 240 & wait $!
'''


def timestamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


logs = common.container_logs
unchanged = common.unchanged


def dropped_packets(output):
    if "BLOCKED=" not in output or "RESTORED=" not in output or "RESTORE FAILED" in output:
        raise RuntimeError("Firewall installation/restoration not confirmed")
    drops = [int(count) for count in re.findall(r"^\s*(\d+)\s+\d+\s+DROP\s", output, re.MULTILINE)]
    if not any(drops):
        raise RuntimeError("No dropped packets recorded; outage was not demonstrated")
    return drops


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True, help="Successful smoke evidence directory")
    parser.add_argument("--timeout", type=common.positive_int, default=240)
    parser.add_argument("--netshoot-image", default="nicolaka/netshoot:latest",
                        help="Already-cached network helper image (tag, digest or ID)")
    parser.add_argument("--allow-network-disruption", action="store_true",
                        help="Allow temporary TCP/30303 isolation in the follower's namespace")
    args = parser.parse_args(argv)
    if not args.allow_network_disruption:
        parser.error("outage testing requires --allow-network-disruption")
    args.run = args.run.resolve()
    details = json.loads((args.run / "run.json").read_text())
    result = json.loads((args.run / "result.json").read_text())
    if details.get("schema_version") != 1 or result.get("passed") is not True:
        raise RuntimeError("Use a successful evidence directory from the maintained common.py")
    follower = common.inspect(details["container"])
    donor = common.inspect(details["donor"])
    if (follower["Id"] == donor["Id"]
            or (follower["Config"].get("Labels") or {}).get("reth.partial-state-lifecycle") != "follower"):
        raise RuntimeError("Refusing to isolate a container not created as a test follower")
    for info in (follower, donor):
        if not info["State"]["Running"] or info["State"]["Paused"]:
            raise RuntimeError(f"Node is not running normally: {info['Name']}")
        if info["Image"] != details["image_id"]:
            raise RuntimeError("Image differs from the successful smoke test")
    network = set(follower["NetworkSettings"]["Networks"]) & set(donor["NetworkSettings"]["Networks"])
    if len(network) != 1:
        raise RuntimeError("Expected exactly one shared devnet network")
    peer = donor["NetworkSettings"]["Networks"][network.pop()]["IPAddress"]
    helper_image = common.inspect(args.netshoot_image)["Id"]
    follower_rpc = common.endpoint(follower, 8545)
    full_rpc = common.endpoint(donor, 8545)
    engine_rpc = common.endpoint(follower, 8551)
    secret = bytes.fromhex((args.run / "jwt.hex").read_text().strip())
    suffix = uuid.uuid4().hex[:10]
    directory = args.run / ("outage-" + suffix)
    directory.mkdir(mode=0o700)
    helper = "reth-ps-outage-" + suffix
    chain = "RPS_" + suffix
    start = timestamp()
    created = False
    restored = False
    common.save(directory / "run.json", {
        "started": start, "follower": follower["Id"], "donor": donor["Id"],
        "image_id": follower["Image"], "helper": helper, "chain": chain, "peer": peer,
        "helper_image_id": helper_image,
        "follower_started_at": follower["State"]["StartedAt"],
        "donor_started_at": donor["State"]["StartedAt"],
    })
    print(f"Evidence: {directory}", flush=True)

    def checkpoint():
        cp = common.db(follower["Id"], "PartialStateCheckpoints", "0", "/data", "/test/genesis.json")
        if not cp or not cp["sync_complete"]:
            raise RuntimeError("No complete verified follower checkpoint")
        return cp

    def head():
        return common.rpc(full_rpc, "eth_getBlockByNumber", ["latest", False])

    def verified(cp, phase):
        return common.verify_checkpoint(cp, full_rpc, directory / f"{phase}-comparison.json")

    def send_target(target, phase):
        response = common.rpc(engine_rpc, "engine_forkchoiceUpdatedV3", [{
            "headBlockHash": target["hash"], "safeBlockHash": common.ZERO,
            "finalizedBlockHash": common.ZERO,
        }, None], secret)
        common.save(directory / f"{phase}-engine.json", response)
        common.verify_syncing(response)

    def wait_checkpoint(target, phase):
        deadline = time.monotonic() + args.timeout
        while time.monotonic() < deadline:
            cp = checkpoint()
            if cp["block_hash"] == target["hash"]:
                header = verified(cp, phase)
                common.save(directory / f"{phase}.json", {"checkpoint": cp, "header": header, "match": True})
                print(f"PASS {phase}: block={cp['block_number']} root_match=true", flush=True)
                return cp
            time.sleep(3)
        raise RuntimeError(f"Timed out waiting for {phase}; see follower.log")

    def restore():
        nonlocal restored
        if common.inspect(helper)["State"]["Running"]:
            common.run("docker", "stop", "--time", "40", helper, timeout=55)
        output = logs(helper)
        (directory / "firewall.log").write_text(output)
        if "RESTORED=" not in output or "RESTORE FAILED" in output:
            raise RuntimeError(f"Firewall cleanup not confirmed. Inspect {helper} and chain {chain}")
        restored = True
        return output

    try:
        previous = checkpoint()
        verified(previous, "before-catch-up")
        target = head()
        common.save(directory / "catch-up-target.json", target)
        gap = int(target["number"], 16) - previous["block_number"]
        if not 0 <= gap <= 128:
            raise RuntimeError("Follower is too far behind for this bounded outage test")
        send_target(target, "catch-up")
        baseline = wait_checkpoint(target, "catch-up")
        common.save(directory / "baseline.json", baseline)
        peers = common.rpc(follower_rpc, "admin_peers", [])
        common.save(directory / "peers-before.json", peers)
        if (len(peers) != 1
                or peers[0]["network"]["remoteAddress"].rsplit(":", 1)[0] != peer
                or peers[0]["protocols"]["eth"]["version"] < 71
                or "snap/1" not in peers[0]["caps"]):
            raise RuntimeError("Expected the donor to be the follower's only eth/71 + snap peer")
        # Do not touch the producer namespace, beacon services, Engine API or RPC traffic.
        common.run("docker", "create", "--init", "--name", helper, "--pull", "never",
                  "--network", f"container:{follower['Id']}", "--cap-add", "NET_ADMIN",
                  "--user", "0", "-e", f"PEER={peer}", "-e", f"CHAIN={chain}",
                  helper_image, "sh", "-ec", FIREWALL)
        created = True
        common.run("docker", "start", helper)
        deadline = time.monotonic() + 15
        while "BLOCKED=" not in logs(helper):
            if time.monotonic() > deadline or not common.inspect(helper)["State"]["Running"]:
                raise RuntimeError("Firewall was not installed")
            time.sleep(1)
        print(logs(helper).strip(), flush=True)
        blocked_head = head()
        deadline = time.monotonic() + 90
        while True:
            target = head()
            if int(target["number"], 16) > int(blocked_head["number"], 16):
                break
            if time.monotonic() > deadline:
                raise RuntimeError("Producer stopped advancing; outage scenario is inconclusive")
            time.sleep(3)
        common.save(directory / "blocked-target.json", target)
        send_target(target, "blocked-target")
        print(f"Target sent while blocked: block={int(target['number'], 16)}", flush=True)
        samples = []
        deadline = time.monotonic() + 65
        while time.monotonic() < deadline:
            firewall = logs(helper)
            if "RESTORED=" in firewall or not common.inspect(helper)["State"]["Running"]:
                raise RuntimeError("Firewall expired before the observation completed")
            cp = checkpoint()
            sample = {"time": timestamp(), "checkpoint": cp, "producer_head": head()["number"]}
            samples.append(sample)
            common.save(directory / "blocked-samples.json", samples)
            if cp != baseline:
                raise RuntimeError("Checkpoint changed while the only peer connection was blocked")
            print(f"Blocked: checkpoint={cp['block_number']} producer_head={int(sample['producer_head'], 16)}", flush=True)
            time.sleep(5)
        progress = int(samples[-1]["producer_head"], 16) > int(target["number"], 16)
        if not progress:
            raise RuntimeError("No further producer progress during outage; test is inconclusive")
        blocked_logs = logs(follower["Id"], start)
        (directory / "blocked-follower.log").write_text(blocked_logs)
        print(f"PASS checkpoint preserved: block={baseline['block_number']}; producer kept advancing", flush=True)
        firewall = restore()
        (directory / "firewall.log").write_text(firewall)
        drops = dropped_packets(firewall)
        print(firewall.strip(), flush=True)
        # No second FCU: recovery must come from the worker's retry of the same target.
        recovered = wait_checkpoint(target, "recovered-same-target")
        next_target = head()
        send_target(next_target, "continued")
        final = wait_checkpoint(next_target, "continued")
        for before in (follower, donor):
            if not unchanged(before, common.inspect(before["Id"])):
                raise RuntimeError("A tested node restarted, stopped or paused")
        finish = common.db(follower["Id"], "StageCheckpoints", '"Finish"', "/data", "/test/genesis.json")
        common.check_execution_boundary(finish, common.rpc(follower_rpc, "eth_blockNumber", []))
        common.save(directory / "result.json", {
            "passed": True, "baseline": baseline, "recovered": recovered, "final": final,
            "same_target_without_second_fcu": True, "nodes_not_restarted": True,
            "producer_advanced_during_outage": True, "dropped_packets": drops,
            "finish": finish,
        })
        print("PASS controlled outage: same-target recovery, matching roots, no node restarts, Finish=0", flush=True)
    except BaseException as error:
        common.save(directory / "failure.json", {"error": str(error), "time": timestamp()})
        raise
    finally:
        try:
            if created and not restored:
                restore()
        finally:
            try:
                (directory / "follower.log").write_text(logs(follower["Id"], start))
            except Exception as error:
                print(f"Could not collect follower logs: {error}", file=sys.stderr)
            print(f"Evidence retained: {directory}", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"FAIL: {error}", file=sys.stderr)
        sys.exit(1)

"""Exercise the host orchestration without Docker, network access or a devnet."""

from contextlib import ExitStack
import copy
import io
import json
from pathlib import Path
import stat
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import common
import outage
import preflight
import smoke


def block(number):
    return {"number": hex(number), "hash": "0x" + f"{number:064x}",
            "stateRoot": "0x" + f"{number + 1000:064x}", "blockAccessListHash": common.ZERO}


def checkpoint(number):
    header = block(number)
    return {"block_number": number, "block_hash": header["hash"],
            "state_root": header["stateRoot"], "sync_complete": True, "filter_hash": common.ZERO}


class Clock:
    def __init__(self):
        self.now = 0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class DockerWorld:
    def __init__(self):
        self.clock = Clock()
        self.commands = []
        self.rpc_calls = []
        self.events = []
        self.cp = checkpoint(9)
        self.head = 9
        self.persisted_reads = 0
        self.starts = 0
        self.checksums = 0
        self.follower = None
        self.helper = None
        self.blocked = False
        self.pending = None
        self.drops = 30
        self.recover = True
        self.change_while_blocked = False
        self.corrupt_root = False
        self.bad_engine = False
        self.bad_checksum = False
        self.full_finish = 0
        self.eth_version = 71
        self.donor = self.node("donor", "donor", "172.16.16.11")

    def node(self, name, identity, address):
        return {
            "Id": identity, "Name": name, "Image": "image-id", "RestartCount": 0,
            "State": {"Running": True, "Paused": False, "StartedAt": "initial"},
            "Config": {"Labels": {"reth.partial-state-lifecycle": "follower"}},
            "NetworkSettings": {
                "Networks": {"devnet": {"IPAddress": address}},
                "Ports": {f"{port}/tcp": [{"HostIp": "127.0.0.1", "HostPort": str(port)}]
                          for port in (8545, 8551)},
            },
        }

    def inspect(self, name):
        if name == "reth-partial-statefulness:local":
            return {"Id": "image-id"}
        if name == "nicolaka/netshoot:latest":
            return {"Id": "helper-image-id"}
        for info in (self.donor, self.follower, self.helper):
            if info and name in (info["Id"], info["Name"]):
                return copy.deepcopy(info)
        raise RuntimeError(f"Unknown test container {name}")

    def run(self, *command, **kwargs):
        self.commands.append(command)
        if command[:2] == ("docker", "create"):
            name = command[command.index("--name") + 1]
            if "NET_ADMIN" in command:
                self.helper = self.node(name, "helper", "172.16.16.6")
                self.helper["State"]["Running"] = False
            else:
                self.follower = self.node(name, "follower", "172.16.16.6")
                self.follower["State"]["Running"] = False
        elif command[:2] == ("docker", "start"):
            if self.helper and command[-1] == self.helper["Name"]:
                self.helper["State"]["Running"] = self.blocked = True
                self.events.append("blocked")
            else:
                self.starts += 1
                self.follower["State"].update(Running=True, StartedAt=f"start-{self.starts}")
                for port in (8545, 8551):
                    self.follower["NetworkSettings"]["Ports"][f"{port}/tcp"][0]["HostPort"] = str(
                        port + self.starts * 10000
                    )
        elif command[:2] == ("docker", "stop"):
            if command[-1] in (self.donor["Id"], self.donor["Name"]):
                raise AssertionError("Test must never stop the donor")
            if self.helper and command[-1] == self.helper["Name"]:
                self.helper["State"]["Running"] = self.blocked = False
                self.events.append("restored")
                if self.pending and self.recover:
                    self.cp = checkpoint(self.pending)
                    self.events.append("recovered-without-fcu")
            else:
                self.follower["State"]["Running"] = False
        elif "checksum" in command:
            self.checksums += 1
            value = "0xbad" if self.bad_checksum and self.checksums > 4 else "0x1234"
            return f"Checksum for table `{command[-1]}`: {value}\n"
        return ""

    def db(self, container, table, key, datadir, chain):
        if container == "donor":
            self.persisted_reads += 1
            return {"block_number": 8 if self.persisted_reads == 1 else 9}
        if table == "StageCheckpoints":
            return {"block_number": self.full_finish}
        cp = copy.deepcopy(self.cp)
        if self.corrupt_root:
            cp["state_root"] = common.ZERO
        if self.blocked and self.change_while_blocked:
            cp["state_root"] = common.ZERO
        return cp

    def rpc(self, url, method, params, secret=None):
        self.rpc_calls.append((url, method))
        if method == "admin_nodeInfo":
            return {"enode": "enode://test@172.16.16.11:30303"}
        if method == "admin_peers":
            return [{"network": {"remoteAddress": "172.16.16.11:30303"},
                     "protocols": {"eth": {"version": self.eth_version}}, "caps": ["eth/71", "snap/1"]}]
        if method == "eth_chainId":
            return "0x123"
        if method == "eth_getBlockByNumber":
            if params[0] == "latest":
                self.head += 1
                return block(self.head)
            return block(int(params[0], 16))
        if method == "eth_getBlockByHash":
            return block(int(params[0], 16))
        if method == "eth_getCode":
            return "0x6000"
        if method == "eth_blockNumber":
            return "0x0"
        if method == "engine_forkchoiceUpdatedV3":
            if url != common.endpoint(self.follower, 8551):
                raise AssertionError("Engine RPC used a stale published port")
            target = int(params[0]["headBlockHash"], 16)
            self.events.append(("fcu", target))
            if self.blocked:
                self.pending = target
            else:
                self.cp = checkpoint(target)
            return {"payloadStatus": {"status": "VALID" if self.bad_engine else "SYNCING",
                                      "latestValidHash": None}, "payloadId": None}
        raise AssertionError(f"Unexpected RPC {method}")

    def logs(self, name, since=None):
        if self.helper and name == self.helper["Name"]:
            start = "BLOCKED=2026-01-01T00:00:00Z\n"
            if self.blocked:
                return start
            return start + f" {self.drops} 5040 DROP all\nRESTORED=2026-01-01T00:02:00Z\n"
        output = "Starting non-validating partial-state follower\nPartial-state snap sync complete\n"
        if self.starts > 1:
            output += "Starting non-validating partial-state follower\nResuming persisted partial-state canonical checkpoint\n"
        return output


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.directory = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        self.world = DockerWorld()
        for module in (common, smoke):
            for name in ("inspect", "run", "db", "rpc", "container_logs"):
                method = self.world.logs if name == "container_logs" else getattr(self.world, name)
                self.stack.enter_context(patch.object(module, name, side_effect=method))
        for module in (smoke, outage):
            self.stack.enter_context(patch.object(module, "time", self.world.clock))
        self.stack.enter_context(patch.object(outage, "logs", side_effect=self.world.logs))
        self.stack.enter_context(patch("sys.stdout", new_callable=io.StringIO))

    def smoke_run(self):
        smoke.main(["--donor", "donor", "--output-dir", str(self.directory), "--timeout", "10"])
        return next(self.directory.iterdir())

    def outage_run(self, directory):
        outage.main(["--run", str(directory), "--timeout", "10", "--allow-network-disruption"])
        return next(directory.glob("outage-*"))

    def test_smoke_bootstrap_advancement_restart_and_evidence(self):
        directory = self.smoke_run()
        result = json.loads((directory / "result.json").read_text())
        self.assertTrue(result["passed"])
        self.assertEqual(result["checkpoint"]["block_number"], 12)
        self.assertEqual(self.world.starts, 2)
        self.assertEqual(self.world.checksums, 8)
        self.assertEqual(stat.S_IMODE((directory / "jwt.hex").stat().st_mode), 0o600)
        self.assertIn("follower = true", (directory / "follower.toml").read_text())
        self.assertNotIn((directory / "jwt.hex").read_text().strip(), (directory / "run.json").read_text())
        self.assertEqual(json.loads((directory / "run.json").read_text())["engine_rpc"], "http://127.0.0.1:28551")
        self.assertIn(("http://127.0.0.1:28545", "eth_blockNumber"), self.world.rpc_calls)
        self.assertTrue(self.world.follower["State"]["Running"])

    def test_smoke_root_mismatch_fails_and_stops_only_new_follower(self):
        self.world.corrupt_root = True
        with self.assertRaisesRegex(RuntimeError, "canonical header"):
            self.smoke_run()
        directory = next(self.directory.iterdir())
        self.assertTrue((directory / "failure.json").exists())
        self.assertFalse((directory / "result.json").exists())
        self.assertFalse(self.world.follower["State"]["Running"])

    def test_smoke_rejects_wrong_image_before_creating_follower(self):
        self.world.donor["Image"] = "stale-image"
        with self.assertRaisesRegex(RuntimeError, "same image"):
            self.smoke_run()
        self.assertEqual(self.world.commands, [])

    def test_smoke_rejects_eth69_fallback(self):
        self.world.eth_version = 69
        with self.assertRaisesRegex(RuntimeError, "negotiate eth/71"):
            self.smoke_run()

    def test_smoke_rejects_valid_engine_status(self):
        self.world.bad_engine = True
        with self.assertRaisesRegex(RuntimeError, "Engine response"):
            self.smoke_run()

    def test_smoke_rejects_full_state_writes(self):
        self.world.bad_checksum = True
        with self.assertRaisesRegex(RuntimeError, "checksums changed"):
            self.smoke_run()

    def test_smoke_rejects_normal_execution_advancement(self):
        self.world.full_finish = 1
        with self.assertRaisesRegex(RuntimeError, "head advanced beyond genesis"):
            self.smoke_run()

    def test_outage_recovers_same_target_without_restarting_nodes(self):
        directory = self.smoke_run()
        self.world.events.clear()
        result_dir = self.outage_run(directory)
        result = json.loads((result_dir / "result.json").read_text())
        self.assertTrue(result["passed"])
        self.assertEqual(result["dropped_packets"], [30])
        self.assertEqual(self.world.starts, 2)
        self.assertFalse(self.world.blocked)
        recovered = self.world.events.index("recovered-without-fcu")
        self.assertEqual(sum(isinstance(e, tuple) for e in self.world.events[:recovered]), 2)
        self.assertEqual(sum(isinstance(e, tuple) for e in self.world.events), 3)
        create = next(c for c in self.world.commands if "NET_ADMIN" in c)
        self.assertEqual(create[create.index("--network") + 1], "container:follower")
        self.assertNotIn("--privileged", create)

    def test_outage_failure_still_restores_connectivity(self):
        directory = self.smoke_run()
        self.world.change_while_blocked = True
        with self.assertRaisesRegex(RuntimeError, "Checkpoint changed"):
            self.outage_run(directory)
        self.assertFalse(self.world.blocked)
        self.assertTrue(self.world.follower["State"]["Running"])
        result_dir = next(directory.glob("outage-*"))
        self.assertTrue((result_dir / "failure.json").exists())
        self.assertFalse((result_dir / "result.json").exists())
        self.assertIn("RESTORED=", (result_dir / "firewall.log").read_text())

    def test_outage_catch_up_header_unavailable_saves_evidence_without_firewall(self):
        directory = self.smoke_run()
        self.world.events.clear()
        def request(url, method, params, secret=None):
            if (method == "eth_getBlockByNumber" and params[0] != "latest"
                    and self.world.events):
                return None
            return self.world.rpc(url, method, params, secret)
        with patch.object(common, "rpc", side_effect=request):
            with self.assertRaisesRegex(RuntimeError, "by_number: header unavailable"):
                self.outage_run(directory)
        result_dir = next(directory.glob("outage-*"))
        comparison = json.loads((result_dir / "catch-up-comparison.json").read_text())
        self.assertIsNone(comparison["by_number"])
        self.assertFalse(comparison["match"])
        target = json.loads((result_dir / "catch-up-target.json").read_text())
        self.assertEqual(comparison["checkpoint"]["block_hash"], target["hash"])
        self.assertFalse((result_dir / "result.json").exists())
        self.assertIsNone(self.world.helper)
        self.assertTrue(self.world.follower["State"]["Running"])

    def test_outage_without_dropped_packets_is_not_a_pass(self):
        directory = self.smoke_run()
        self.world.drops = 0
        with self.assertRaisesRegex(RuntimeError, "No dropped packets"):
            self.outage_run(directory)
        self.assertFalse(self.world.blocked)

    def test_outage_without_recovery_is_not_a_pass(self):
        directory = self.smoke_run()
        self.world.recover = False
        with self.assertRaisesRegex(RuntimeError, "Timed out waiting"):
            self.outage_run(directory)
        self.assertFalse(self.world.blocked)

    def test_outage_interrupt_restores_connectivity(self):
        directory = self.smoke_run()
        def read(*args):
            if self.world.blocked:
                raise KeyboardInterrupt()
            return self.world.db(*args)
        with patch.object(common, "db", side_effect=read), self.assertRaises(KeyboardInterrupt):
            self.outage_run(directory)
        self.assertFalse(self.world.blocked)
        self.assertTrue(self.world.follower["State"]["Running"])
        self.assertEqual(self.world.starts, 2)
        result_dir = next(directory.glob("outage-*"))
        self.assertTrue((result_dir / "failure.json").exists())
        self.assertFalse((result_dir / "result.json").exists())
        self.assertIn("RESTORED=", (result_dir / "firewall.log").read_text())

    def test_outage_unconfirmed_cleanup_is_not_a_pass(self):
        directory = self.smoke_run()
        def log(name, since=None):
            return self.world.logs(name, since).replace("RESTORED=", "RESTORE FAILED=")
        with patch.object(outage, "logs", side_effect=log):
            with self.assertRaisesRegex(RuntimeError, "Firewall cleanup not confirmed"):
                self.outage_run(directory)
        result_dir = next(directory.glob("outage-*"))
        self.assertFalse((result_dir / "result.json").exists())
        self.assertTrue((result_dir / "follower.log").exists())

    def test_outage_stalled_producer_is_not_a_pass(self):
        directory = self.smoke_run()
        def request(url, method, params, secret=None):
            if self.world.blocked and method == "eth_getBlockByNumber" and params[0] == "latest":
                return block(self.world.head)
            return self.world.rpc(url, method, params, secret)
        with patch.object(common, "rpc", side_effect=request):
            with self.assertRaisesRegex(RuntimeError, "Producer stopped advancing"):
                self.outage_run(directory)
        self.assertFalse(self.world.blocked)
        self.assertFalse(list(directory.glob("outage-*/result.json")))

    def test_outage_requires_explicit_permission(self):
        with patch("sys.stderr", new_callable=io.StringIO), self.assertRaises(SystemExit):
            outage.main(["--run", str(self.directory)])
        self.assertEqual(self.world.commands, [])

    def test_outage_refuses_an_unsuccessful_smoke_run(self):
        directory = self.smoke_run()
        common.save(directory / "result.json", {"passed": False})
        with self.assertRaisesRegex(RuntimeError, "successful evidence"):
            self.outage_run(directory)
        self.assertIsNone(self.world.helper)

    def test_outage_refuses_non_test_containers(self):
        directory = self.smoke_run()
        self.world.follower["Config"]["Labels"] = {}
        with self.assertRaisesRegex(RuntimeError, "not created as a test follower"):
            self.outage_run(directory)
        self.assertIsNone(self.world.helper)


class InvariantTests(unittest.TestCase):
    def test_failed_checkpoint_comparisons_preserve_both_responses_without_retry(self):
        cases = [
            (None, block(9), "by_hash: header unavailable"),
            (block(9), None, "by_number: header unavailable"),
            (block(9), dict(block(9), hash=common.ZERO), "by_number: block hash differs"),
            (dict(block(9), number="0x8"), block(9), "by_hash: block number differs"),
            (dict(block(9), stateRoot=common.ZERO), block(9), "by_hash: state root differs"),
        ]
        for header, canonical, reason in cases:
            with self.subTest(reason=reason), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "comparison.json"
                with patch.object(common, "rpc", side_effect=[header, canonical]) as rpc:
                    with self.assertRaisesRegex(RuntimeError, reason):
                        common.verify_checkpoint(checkpoint(9), "http://localhost", path)
                self.assertEqual(rpc.call_count, 2)
                evidence = json.loads(path.read_text())
                self.assertEqual(evidence["checkpoint"], checkpoint(9))
                self.assertEqual(evidence["by_hash"], header)
                self.assertEqual(evidence["by_number"], canonical)
                self.assertEqual(evidence["mismatches"], [reason + " (null)" if canonical is None or header is None else reason])
                self.assertFalse(evidence["match"])

    def test_matching_checkpoint_comparison_records_success(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "comparison.json"
            with patch.object(common, "rpc", side_effect=[block(9), block(9)]):
                self.assertEqual(common.verify_checkpoint(checkpoint(9), "http://localhost", path), block(9))
            evidence = json.loads(path.read_text())
            self.assertTrue(evidence["match"])
            self.assertEqual(evidence["mismatches"], [])

    def test_malformed_header_still_leaves_failed_comparison(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "comparison.json"
            with patch.object(common, "rpc", side_effect=[{}, block(9)]):
                with self.assertRaises(KeyError):
                    common.verify_checkpoint(checkpoint(9), "http://localhost", path)
            evidence = json.loads(path.read_text())
            self.assertEqual(evidence["by_hash"], {})
            self.assertFalse(evidence["match"])

    def test_root_check_rejects_absent_stale_and_noncanonical_headers(self):
        for header, canonical in [(None, block(9)), (block(8), block(9)),
                                  (block(9), block(8)), (block(9), None)]:
            with self.subTest(header=header, canonical=canonical):
                with patch.object(common, "rpc", side_effect=[header, canonical]):
                    with self.assertRaisesRegex(RuntimeError, "canonical header"):
                        common.verify_checkpoint(checkpoint(9), "http://localhost")

    def test_engine_must_not_return_valid_hash_or_payload(self):
        base = {"payloadStatus": {"status": "SYNCING", "latestValidHash": None}, "payloadId": None}
        common.verify_syncing(base)
        for field in ("latestValidHash", "payloadId"):
            response = copy.deepcopy(base)
            target = response["payloadStatus"] if field == "latestValidHash" else response
            target[field] = common.ZERO
            with self.assertRaisesRegex(RuntimeError, "Engine response"):
                common.verify_syncing(response)

    def test_restart_and_pause_are_detected(self):
        world = DockerWorld()
        self.assertTrue(common.unchanged(world.donor, copy.deepcopy(world.donor)))
        for location, key, value in [("State", "StartedAt", "new"), ("State", "Paused", True),
                                     ("State", "Running", False), (None, "RestartCount", 1)]:
            changed = copy.deepcopy(world.donor)
            (changed[location] if location else changed)[key] = value
            self.assertFalse(common.unchanged(world.donor, changed))

    def test_firewall_requires_drops_and_restoration(self):
        for output in ("BLOCKED=x\n 3 10 DROP all", "BLOCKED=x\nRESTORED=y\n 0 0 DROP all",
                       "BLOCKED=x\nRESTORED=y\n 3 10 DROP all\nRESTORE FAILED"):
            with self.assertRaises(RuntimeError):
                outage.dropped_packets(output)

    def test_firewall_syntax_and_expiry(self):
        subprocess.run(["sh", "-n"], input=outage.FIREWALL, text=True, check=True)
        self.assertIn("sleep 240", outage.FIREWALL)
        self.assertIn("trap cleanup EXIT", outage.FIREWALL)
        self.assertIn('iptables -w 5 -D INPUT -s "$PEER"', outage.FIREWALL)
        self.assertIn('iptables -w 5 -D OUTPUT -d "$PEER"', outage.FIREWALL)

    def test_preflight_stalled_head_never_passes(self):
        world = DockerWorld()
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            stack.enter_context(patch.object(preflight, "inspect", side_effect=world.inspect))
            stack.enter_context(patch.object(preflight, "time", world.clock))
            stack.enter_context(patch.object(preflight, "rpc", return_value=block(9)))
            stack.enter_context(patch.object(preflight, "db", return_value={"block_number": 9}))
            stack.enter_context(patch("sys.stdout", new_callable=io.StringIO))
            with self.assertRaisesRegex(RuntimeError, "readiness not demonstrated"):
                preflight.main(["--donor", "donor", "--timeout", "10", "--interval", "1", "--output-dir", directory])
            self.assertFalse(list(Path(directory).glob("*/result.json")))

    def test_preflight_requires_head_and_persistence_progress(self):
        for advancing_finish in (True, False):
            world = DockerWorld()
            with self.subTest(advancing_finish=advancing_finish), \
                    tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
                stack.enter_context(patch.object(preflight, "inspect", side_effect=world.inspect))
                stack.enter_context(patch.object(preflight, "time", world.clock))
                stack.enter_context(patch.object(preflight, "rpc", side_effect=world.rpc))
                stack.enter_context(patch.object(
                    preflight, "db", side_effect=world.db if advancing_finish else None,
                    return_value={"block_number": 9},
                ))
                stack.enter_context(patch("sys.stdout", new_callable=io.StringIO))
                args = ["--donor", "donor", "--timeout", "10", "--interval", "1", "--output-dir", directory]
                if advancing_finish:
                    preflight.main(args)
                    result = next(Path(directory).glob("*/result.json"))
                    self.assertTrue(json.loads(result.read_text())["passed"])
                else:
                    with self.assertRaisesRegex(RuntimeError, "readiness not demonstrated"):
                        preflight.main(args)
                    self.assertFalse(list(Path(directory).glob("*/result.json")))


if __name__ == "__main__":
    unittest.main()

"""Host-only tests of the RPC boundary scenario, including false-positive rejection."""

from contextlib import ExitStack
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import common
import rpc_boundaries as boundaries
from test_lifecycle import DockerWorld, checkpoint


def success(value):
    return {"jsonrpc": "2.0", "id": 1, "result": value}


def missing(code):
    kind = "storage" if code == -32001 else "bytecode"
    return {"jsonrpc": "2.0", "id": 1, "error": {
        "code": code,
        "message": f"{kind} for address {boundaries.UNTRACKED} is not tracked by this partial-state node",
    }}


class BoundaryTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.directory = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        self.world = DockerWorld()
        self.world.follower = self.world.node("follower", "follower", "172.16.16.10")
        self.world.follower["NetworkSettings"]["Ports"]["8545/tcp"][0]["HostPort"] = "18545"
        self.at = {"blockHash": self.world.cp["block_hash"]}
        common.save(self.directory / "run.json", {
            "schema_version": 1, "container": "follower", "donor": "donor", "image_id": "image-id",
            "tracked_contract": common.TRACKED, "call_data": "0x621fd130",
        })
        common.save(self.directory / "result.json", {"passed": True, "checkpoint_calls_checked": True})
        for name in ("inspect", "db", "rpc", "run"):
            self.stack.enter_context(patch.object(common, name, side_effect=getattr(self.world, name)))
        self.response = self.stack.enter_context(patch.object(common, "rpc_response", side_effect=self.respond))
        self.stdout = self.stack.enter_context(patch("sys.stdout", new_callable=io.StringIO))

    def respond(self, url, method, params):
        self.assertEqual(params[1] if method in ("eth_call", "eth_estimateGas", "eth_getCode") else params[2], self.at)
        follower = url == common.endpoint(self.world.follower, 8545)
        target = (params[0]["to"] if method in ("eth_call", "eth_estimateGas") else params[0]).lower()
        tracked = common.TRACKED.lower()
        if method == "eth_getProof":
            self.assertFalse(follower)
            return success({"storageHash": boundaries.EMPTY_ROOT if target == boundaries.EMPTY else "0x" + "11" * 32})
        if method == "eth_getCode":
            if follower and target == boundaries.UNTRACKED:
                return missing(-32002)
            return success("0x" if target == boundaries.EMPTY else "0x60005400")
        if method == "eth_getStorageAt":
            if follower and target == boundaries.UNTRACKED:
                return missing(-32001)
            return success(common.ZERO)
        if method in ("eth_call", "eth_estimateGas"):
            overrides = params[2] if len(params) > 2 else {}
            code = overrides.get(tracked, {}).get("code", "")
            nested = boundaries.UNTRACKED[2:] in code
            if follower and (target == boundaries.UNTRACKED or nested):
                return missing(-32002)
            if method == "eth_estimateGas":
                return success("0x6000")
            if nested:
                return success("0x" + f"{42:064x}")
            if target == boundaries.EMPTY:
                return success("0x")
            if target == boundaries.UNTRACKED:
                return success(common.ZERO)
            if overrides:
                override = overrides[tracked]
                if "stateDiff" in override:
                    return success(override["stateDiff"][common.ZERO])
                return success("0x" + code[4:68])
            return success("0x" + f"{7:064x}")
        raise AssertionError(f"Unexpected RPC {method}")

    def execute(self):
        boundaries.main(["--run", str(self.directory)])
        return self.evidence()

    def evidence(self):
        return next(self.directory.glob("rpc-boundaries-*"))

    def assert_failed(self, reason):
        with self.assertRaisesRegex(RuntimeError, reason):
            self.execute()
        self.assertFalse((self.evidence() / "result.json").exists())
        self.assertTrue((self.evidence() / "failure.json").exists())
        self.assertTrue(self.world.follower["State"]["Running"])
        self.assertTrue(self.world.donor["State"]["Running"])

    def test_errors_overrides_empty_state_and_unchanged_tables(self):
        evidence = self.execute()
        result = json.loads((evidence / "result.json").read_text())
        self.assertTrue(result["passed"])
        self.assertEqual(result["checkpoint"], checkpoint(9))
        self.assertEqual(result["tables_checked"], list(boundaries.STATE_TABLES))
        self.assertEqual(self.world.checksums, 2 * len(boundaries.STATE_TABLES))
        self.assertTrue(all(c[:2] == ("docker", "exec") and "checksum" in c for c in self.world.commands))
        self.assertTrue(all("--quiet" not in c for c in self.world.commands))
        observations = json.loads((evidence / "rpc-observations.json").read_text())
        errors = [o for o in observations if "error" in o["response"]]
        self.assertEqual(len(errors), 8)
        self.assertEqual({o["response"]["error"]["code"] for o in errors}, {-32001, -32002})
        self.assertIn("PASS RPC boundaries", self.stdout.getvalue())

    def test_success_instead_of_missing_state_is_not_a_pass(self):
        for value in ("0x", common.ZERO, "0x6000"):
            with self.subTest(value=value):
                response = success(value)
                with self.assertRaisesRegex(RuntimeError, "expected -32002"):
                    boundaries.require_missing(response, -32002, boundaries.UNTRACKED, "test")
        def respond(*args):
            response = self.respond(*args)
            return success("0x") if "error" in response else response
        self.response.side_effect = respond
        self.assert_failed("expected -32002")

    def test_wrong_error_code_message_and_address_are_not_a_pass(self):
        for response in [
            {"error": {"code": -32602, "message": "bad params"}},
            {"error": {"code": -32002, "message": "execution reverted"}},
            {"error": {"code": -32002, "message": missing(-32002)["error"]["message"].replace(boundaries.UNTRACKED, common.TRACKED)}},
            dict(missing(-32002), result="0x"),
        ]:
            with self.subTest(response=response), self.assertRaises(RuntimeError):
                boundaries.require_missing(response, -32002, boundaries.UNTRACKED, "test")

    def test_nested_call_swallowing_missing_state_is_rejected(self):
        def respond(url, method, params):
            response = self.respond(url, method, params)
            if ("error" in response and method == "eth_call"
                    and params[0]["to"].lower() == common.TRACKED.lower()):
                return success(common.ZERO)
            return response
        self.response.side_effect = respond
        self.assert_failed("expected -32002")

    def test_invalid_producer_control_is_not_a_pass(self):
        def respond(url, method, params):
            if (url == common.endpoint(self.world.donor, 8545) and method == "eth_call"
                    and len(params) > 2):
                return {"error": {"code": 3, "message": "execution reverted"}}
            return self.respond(url, method, params)
        self.response.side_effect = respond
        self.assert_failed("unexpected RPC error")

    def test_empty_untracked_fixture_is_rejected(self):
        def respond(url, method, params):
            if method == "eth_getProof" and params[0] == boundaries.UNTRACKED:
                return success({"storageHash": boundaries.EMPTY_ROOT})
            return self.respond(url, method, params)
        self.response.side_effect = respond
        self.assert_failed("Invalid untracked fixture")

    def test_proof_window_refusal_explains_setup_without_latest_fallback(self):
        error = {"code": -32602, "message": "distance to target block exceeds maximum proof window"}
        def respond(url, method, params):
            if method == "eth_getProof":
                self.assertEqual(params[2], self.at)
                return {"jsonrpc": "2.0", "id": 1, "error": error}
            return self.respond(url, method, params)
        self.response.side_effect = respond
        self.assert_failed("Producer refused eth_getProof at checkpoint 9.*--rpc.eth-proof-window=4096")
        observations = json.loads((self.evidence() / "rpc-observations.json").read_text())
        self.assertEqual([o["method"] for o in observations], ["eth_getCode", "eth_getProof"])
        self.assertTrue(all(o["peer"] == "producer" for o in observations))
        self.assertEqual(observations[-1]["response"]["error"], error)
        self.assertEqual(self.world.commands, [])
        self.assertEqual(self.world.checksums, 0)
        self.assertNotIn("PASS", self.stdout.getvalue())

    def test_unrelated_proof_errors_are_not_reported_as_window_errors(self):
        def respond(url, method, params):
            if method == "eth_getProof":
                return {"jsonrpc": "2.0", "id": 1, "error": {"code": -32602, "message": "bad params"}}
            return self.respond(url, method, params)
        self.response.side_effect = respond
        self.assert_failed("fixture-storage: unexpected RPC error.*bad params")

    def test_missing_checksum_is_not_a_pass(self):
        with patch.object(common, "run", return_value=""):
            self.assert_failed("Missing or ambiguous checksum")

    def test_partial_state_writes_are_not_a_pass(self):
        def run(*command, **kwargs):
            output = self.world.run(*command, **kwargs)
            if self.world.checksums > len(boundaries.STATE_TABLES) and command[-1] == "PartialStateStorages":
                return output.replace("0x1234", "0xbad")
            return output
        with patch.object(common, "run", side_effect=run):
            self.assert_failed("Persisted table checksums changed")

    def test_checkpoint_change_is_not_a_pass(self):
        def respond(*args):
            response = self.respond(*args)
            if "error" in response:
                self.world.cp = checkpoint(10)
            return response
        self.response.side_effect = respond
        self.assert_failed("Checkpoint changed")

    def test_node_restart_is_not_a_pass(self):
        def respond(*args):
            response = self.respond(*args)
            if "error" in response:
                self.world.follower["State"]["StartedAt"] = "restarted"
            return response
        self.response.side_effect = respond
        self.assert_failed("restarted, stopped or paused")

    def test_transport_failure_is_not_a_missing_state_pass(self):
        def respond(*args):
            if "error" in self.respond(*args):
                raise TimeoutError("RPC timed out")
            return self.respond(*args)
        self.response.side_effect = respond
        with self.assertRaises(TimeoutError):
            self.execute()
        observations = json.loads((self.evidence() / "rpc-observations.json").read_text())
        self.assertIn("transport_or_protocol_error", observations[-1])
        self.assertFalse((self.evidence() / "result.json").exists())

    def test_requires_successful_calls_and_test_container(self):
        common.save(self.directory / "result.json", {"passed": True})
        with self.assertRaisesRegex(RuntimeError, "smoke --call-data"):
            self.execute()
        common.save(self.directory / "result.json", {"passed": True, "checkpoint_calls_checked": True})
        self.world.follower["Config"]["Labels"] = {}
        with self.assertRaisesRegex(RuntimeError, "not created as a test follower"):
            self.execute()
        self.assertEqual(self.world.commands, [])

    def test_empty_or_incomplete_run_is_rejected_before_docker(self):
        (self.directory / "result.json").unlink()
        for value, message in [("", "--run is empty"), (str(self.directory), "failed or incomplete")]:
            with self.subTest(value=value), patch("sys.stderr", new_callable=io.StringIO) as stderr:
                with self.assertRaises(SystemExit) as raised:
                    boundaries.main(["--run", value])
                self.assertEqual(raised.exception.code, 2)
                self.assertIn(message, stderr.getvalue())
        self.assertEqual(self.world.commands, [])


class ResponseTests(unittest.TestCase):
    def request(self, response, raw=False):
        with patch.object(common.urllib.request, "urlopen", return_value=io.StringIO(json.dumps(response))):
            return (common.rpc_response if raw else common.rpc)("http://localhost", "eth_call", [])

    def test_expected_errors_remain_structured_but_normal_rpc_still_raises(self):
        self.assertEqual(self.request(missing(-32001), raw=True), missing(-32001))
        with self.assertRaisesRegex(RuntimeError, "not tracked"):
            self.request(missing(-32001))
        self.assertEqual(self.request(success("0x")), "0x")

    def test_malformed_envelopes_are_rejected(self):
        for response in [None, [], {}, {"jsonrpc": "2.0", "id": 1},
                         dict(success("0x"), id=2), dict(success("0x"), id=True),
                         dict(success("0x"), jsonrpc="1.0"), dict(missing(-32001), result="0x"),
                         {"jsonrpc": "2.0", "id": 1, "error": None},
                         {"jsonrpc": "2.0", "id": 1, "error": {"code": True, "message": "bad"}}]:
            with self.subTest(response=response), self.assertRaisesRegex(RuntimeError, "Malformed JSON-RPC"):
                self.request(response, raw=True)


if __name__ == "__main__":
    unittest.main()

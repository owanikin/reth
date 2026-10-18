"""Host-only regression tests; no real process is killed by this suite."""

from contextlib import ExitStack
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import urllib.error

import abrupt_restart
import common
from test_lifecycle import DockerWorld, block, checkpoint


class AbruptWorld(DockerWorld):
    def __init__(self, directory):
        super().__init__()
        self.starts = 1
        self.follower = self.node("reth-ps-follower-test", "follower", "172.16.16.10")
        self.follower["State"]["StartedAt"] = "start-1"
        self.follower["NetworkSettings"]["Ports"]["8545/tcp"][0]["HostPort"] = "18545"
        self.follower["HostConfig"] = {"AutoRemove": False, "RestartPolicy": {"Name": "no"}}
        self.follower["Mounts"] = [
            {"Destination": "/data", "Type": "volume", "Name": "test-data", "RW": True},
            {"Destination": "/test", "Type": "bind", "Source": str(directory), "RW": False},
        ]

    def run(self, *command, **kwargs):
        if command[:2] == ("docker", "kill"):
            if command != ("docker", "kill", "--signal", "SIGKILL", "follower"):
                raise AssertionError("Only SIGKILL of the exact test follower is authorized")
            self.commands.append(command)
            self.follower["State"].update(Running=False, ExitCode=137, OOMKilled=False,
                                          FinishedAt="2026-01-01T00:00:01Z")
            return "follower\n"
        if command[:2] == ("docker", "start"):
            if command[-1] != "follower":
                raise AssertionError("Only the same test follower may be restarted")
        if command[:2] in (("docker", "stop"), ("docker", "rm"), ("docker", "create"),
                          ("docker", "run"), ("docker", "restart")):
            raise AssertionError("No graceful stops, data replacement or helper containers")
        return super().run(*command, **kwargs)

    def rpc(self, url, method, params, secret=None):
        if method in ("eth_getCode", "eth_getStorageAt", "eth_call", "eth_estimateGas"):
            if params[-1] != {"blockHash": self.cp["block_hash"]}:
                raise AssertionError("RPC reads must use the exact committed checkpoint")
            if url not in (common.endpoint(self.donor, 8545), common.endpoint(self.follower, 8545)):
                raise AssertionError("RPC used a stale port after restart")
            return {"eth_getCode": "0x6000", "eth_getStorageAt": common.ZERO,
                    "eth_call": "0x1234", "eth_estimateGas": "0x6000"}[method]
        return super().rpc(url, method, params, secret)

    def logs(self, container, since=None):
        if since:
            if since != self.follower["State"]["FinishedAt"]:
                raise AssertionError("Resume evidence must come after the kill, not old startup logs")
            return "Resuming persisted partial-state canonical checkpoint\n"
        return super().logs(container)


class AbruptRestartTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.directory = Path(self.stack.enter_context(tempfile.TemporaryDirectory())).resolve()
        self.world = AbruptWorld(self.directory)
        common.save(self.directory / "run.json", {
            "schema_version": 1, "container": self.world.follower["Name"], "volume": "test-data",
            "donor": "donor", "image_id": "image-id", "tracked_contract": common.TRACKED,
            "call_data": "0x621fd130",
        })
        common.save(self.directory / "result.json", {"passed": True, "checkpoint_calls_checked": True})
        common.save(self.directory / "before-checksums.json", {t: "0x1234" for t in common.FULL_TABLES})
        (self.directory / "jwt.hex").write_text("11" * 32)
        for name in ("inspect", "db", "rpc", "run", "container_logs"):
            method = self.world.logs if name == "container_logs" else getattr(self.world, name)
            self.stack.enter_context(patch.object(common, name, side_effect=method))
        self.stack.enter_context(patch.object(abrupt_restart, "time", self.world.clock))
        self.stdout = self.stack.enter_context(patch("sys.stdout", new_callable=io.StringIO))

    def execute(self):
        abrupt_restart.main(["--run", str(self.directory), "--timeout", "5", "--allow-abrupt-stop"])
        return self.evidence()

    def evidence(self):
        return next(self.directory.glob("abrupt-restart-*"))

    def assert_failed(self, message, error=RuntimeError):
        with self.assertRaisesRegex(error, message):
            self.execute()
        self.assertFalse((self.evidence() / "result.json").exists())
        self.assertTrue((self.evidence() / "failure.json").exists())
        self.assertTrue(self.world.donor["State"]["Running"])
        self.assertEqual(self.world.donor["State"]["StartedAt"], "initial")

    def kills(self):
        return [c for c in self.world.commands if c[:2] == ("docker", "kill")]

    def test_committed_checkpoint_survives_sigkill_and_advances_on_same_volume(self):
        directory = self.execute()
        result = json.loads((directory / "result.json").read_text())
        self.assertTrue(result["passed"])
        self.assertEqual(result["baseline"], checkpoint(10))
        self.assertEqual(result["recovered"], result["baseline"])
        self.assertEqual(result["final"], checkpoint(11))
        self.assertEqual(result["tables_checked"], list(common.STATE_TABLES))
        self.assertEqual(self.world.checksums, 2 * len(common.STATE_TABLES) + len(common.FULL_TABLES))
        self.assertEqual(self.world.starts, 2)
        self.assertEqual(self.kills(), [("docker", "kill", "--signal", "SIGKILL", "follower")])
        mutations = [c for c in self.world.commands if c[:2] != ("docker", "exec")]
        self.assertEqual(mutations, [*self.kills(), ("docker", "start", "follower")])
        self.assertTrue(self.world.follower["State"]["Running"])
        self.assertEqual(len(self.world.events), 2)
        self.assertIn("PASS abrupt restart", self.stdout.getvalue())
        self.assertEqual(json.loads((directory / "follower-killed.json").read_text())["State"]["ExitCode"], 137)
        self.assertEqual(json.loads((directory / "restarted-rpc.json").read_text())["http"], "http://127.0.0.1:28545")
        self.assertTrue((directory / "follower.log").exists())

    def test_requires_explicit_permission_before_docker(self):
        with patch("sys.stderr", new_callable=io.StringIO), self.assertRaises(SystemExit):
            abrupt_restart.main(["--run", str(self.directory)])
        common.inspect.assert_not_called()
        self.assertEqual(self.world.commands, [])

    def test_empty_run_is_rejected_before_path_resolution_or_docker(self):
        for value in ("", "   "):
            with self.subTest(value=value), patch("sys.stderr", new_callable=io.StringIO) as stderr:
                with self.assertRaises(SystemExit) as raised:
                    abrupt_restart.main(["--run", value, "--allow-abrupt-stop"])
                self.assertEqual(raised.exception.code, 2)
                self.assertIn("--run is empty; set RUN", stderr.getvalue())
        common.inspect.assert_not_called()
        self.assertEqual(self.world.commands, [])

    def test_incomplete_evidence_is_rejected_before_docker(self):
        for name in ("run.json", "result.json", "before-checksums.json", "jwt.hex"):
            original = (self.directory / name).read_text()
            (self.directory / name).unlink()
            with self.subTest(name=name), patch("sys.stderr", new_callable=io.StringIO) as stderr:
                with self.assertRaises(SystemExit) as raised:
                    self.execute()
                self.assertEqual(raised.exception.code, 2)
                self.assertIn("not a complete smoke evidence directory", stderr.getvalue())
                self.assertIn(f"missing {name}", stderr.getvalue())
            (self.directory / name).write_text(original)
        common.inspect.assert_not_called()
        self.assertEqual(self.world.commands, [])

    def test_requires_successful_smoke_with_calls(self):
        common.save(self.directory / "result.json", {"passed": True})
        with self.assertRaisesRegex(RuntimeError, "successful smoke --call-data"):
            self.execute()
        self.assertEqual(self.world.commands, [])

    def test_refuses_unlabelled_or_wrong_container(self):
        self.world.follower["Config"]["Labels"] = {}
        with self.assertRaisesRegex(RuntimeError, "not created as a test follower"):
            self.execute()
        self.assertEqual(self.world.commands, [])

    def test_refuses_donor_as_follower(self):
        self.world.follower["Id"] = "donor"
        with self.assertRaisesRegex(RuntimeError, "not created as a test follower"):
            self.execute()
        self.assertEqual(self.world.commands, [])

    def test_refuses_changed_volume_or_bind_mount(self):
        for mount, field, value in [(0, "Name", "different-data"), (1, "RW", True),
                                    (1, "Source", "/not-the-smoke-run")]:
            with self.subTest(field=field):
                original = self.world.follower["Mounts"][mount][field]
                self.world.follower["Mounts"][mount][field] = value
                with self.assertRaisesRegex(RuntimeError, "mounts do not match"):
                    self.execute()
                self.world.follower["Mounts"][mount][field] = original
        self.assertEqual(self.world.commands, [])

    def test_accepts_docker_desktop_macos_bind_path(self):
        self.world.follower["Mounts"][1]["Source"] = "/host_mnt" + str(self.directory)
        with patch.object(abrupt_restart.sys, "platform", "darwin"):
            evidence = self.execute()
        self.assertTrue(json.loads((evidence / "result.json").read_text())["passed"])
        self.assertEqual(len(self.kills()), 1)

    def test_desktop_path_translation_does_not_relax_mount_safety(self):
        self.world.follower["Mounts"][1]["Source"] = "/host_mnt" + str(self.directory)
        with patch.object(abrupt_restart.sys, "platform", "darwin"):
            for mount, field, value in [(0, "Name", "different-data"), (1, "RW", True),
                                        (1, "Type", "volume")]:
                with self.subTest(field=field):
                    original = self.world.follower["Mounts"][mount][field]
                    self.world.follower["Mounts"][mount][field] = value
                    with self.assertRaisesRegex(RuntimeError, "mounts do not match"):
                        self.execute()
                    self.world.follower["Mounts"][mount][field] = original
        self.assertEqual(self.world.commands, [])

    def test_unrelated_bind_prefixes_and_directories_are_not_accepted(self):
        with patch.object(abrupt_restart.sys, "platform", "darwin"):
            for source in [None, "", str(self.directory).lstrip("/"),
                           "/another_vm" + str(self.directory),
                           "/host_mnt_other" + str(self.directory),
                           "/host_mnt" + str(self.directory) + "-different"]:
                with self.subTest(source=source):
                    self.world.follower["Mounts"][1]["Source"] = source
                    with self.assertRaisesRegex(RuntimeError, "mounts do not match"):
                        self.execute()
        self.assertEqual(self.world.commands, [])

    def test_does_not_translate_host_mnt_on_linux(self):
        self.world.follower["Mounts"][1]["Source"] = "/host_mnt" + str(self.directory)
        with patch.object(abrupt_restart.sys, "platform", "linux"):
            with self.assertRaisesRegex(RuntimeError, "mounts do not match"):
                self.execute()
        self.assertEqual(self.world.commands, [])

    def test_refuses_auto_remove_and_auto_restart(self):
        self.world.follower["HostConfig"]["AutoRemove"] = True
        with self.assertRaisesRegex(RuntimeError, "disable auto-remove"):
            self.execute()
        self.world.follower["HostConfig"]["AutoRemove"] = False
        self.world.follower["HostConfig"]["RestartPolicy"]["Name"] = "always"
        with self.assertRaisesRegex(RuntimeError, "automatic restarts"):
            self.execute()
        self.assertEqual(self.world.commands, [])

    def test_refuses_wrong_image_or_stopped_node(self):
        self.world.donor["Image"] = "other-image"
        with self.assertRaisesRegex(RuntimeError, "Image differs"):
            self.execute()
        self.world.donor["Image"] = "image-id"
        self.world.donor["State"]["Running"] = False
        with self.assertRaisesRegex(RuntimeError, "not running normally"):
            self.execute()
        self.assertEqual(self.world.commands, [])

    def test_rpc_mismatch_before_kill_is_not_a_pass(self):
        def rpc(url, method, params, secret=None):
            if method == "eth_call" and url == common.endpoint(self.world.follower, 8545):
                return "0xbad0"
            return self.world.rpc(url, method, params, secret)
        with patch.object(common, "rpc", side_effect=rpc):
            self.assert_failed("RPC comparison failed")
        self.assertEqual(self.kills(), [])

    def test_full_state_changes_after_reopen_are_rejected(self):
        self.world.bad_checksum = True
        self.assert_failed("Full-state table checksums")
        self.assertEqual(len(self.kills()), 1)

    def test_full_state_changes_before_kill_are_rejected(self):
        def run(*command, **kwargs):
            output = self.world.run(*command, **kwargs)
            if command[-1] == "PlainStorageState":
                return output.replace("0x1234", "0xbad")
            return output
        with patch.object(common, "run", side_effect=run):
            self.assert_failed("Full-state table checksums")
        self.assertEqual(self.kills(), [])

    def test_checkpoint_change_during_checksums_prevents_kill(self):
        def run(*command, **kwargs):
            result = self.world.run(*command, **kwargs)
            if "checksum" in command:
                self.world.cp = checkpoint(12)
            return result
        with patch.object(common, "run", side_effect=run):
            self.assert_failed("Checkpoint changed unexpectedly")
        self.assertEqual(self.kills(), [])

    def test_kill_must_be_confirmed_not_a_graceful_exit(self):
        def run(*command, **kwargs):
            result = self.world.run(*command, **kwargs)
            if command[:2] == ("docker", "kill"):
                self.world.follower["State"]["ExitCode"] = 0
            return result
        with patch.object(common, "run", side_effect=run):
            self.assert_failed("SIGKILL termination was not confirmed")
        self.assertTrue(self.world.follower["State"]["Running"])
        self.assertTrue((self.evidence() / "cleanup.json").exists())

    def test_oom_is_not_accepted_as_a_controlled_kill(self):
        def run(*command, **kwargs):
            result = self.world.run(*command, **kwargs)
            if command[:2] == ("docker", "kill"):
                self.world.follower["State"]["OOMKilled"] = True
            return result
        with patch.object(common, "run", side_effect=run):
            self.assert_failed("SIGKILL termination was not confirmed")

    def test_waits_for_stopped_container_after_signal(self):
        polls = 0
        def inspect(name):
            nonlocal polls
            info = self.world.inspect(name)
            if info["Id"] == "follower" and info["State"].get("ExitCode") == 137 and self.world.starts == 1:
                polls += 1
                if polls <= 2:
                    info["State"]["Running"] = True
            return info
        with patch.object(common, "inspect", side_effect=inspect):
            self.execute()
        self.assertEqual(polls, 3)

    def test_signal_without_termination_is_not_a_pass(self):
        def run(*command, **kwargs):
            output = self.world.run(*command, **kwargs)
            if command[:2] == ("docker", "kill"):
                self.world.follower["State"]["Running"] = True
            return output
        with patch.object(common, "run", side_effect=run):
            self.assert_failed("SIGKILL termination was not confirmed")
        self.assertEqual(self.world.starts, 1)

    def test_interrupt_after_kill_attempts_one_restart_but_never_passes(self):
        def run(*command, **kwargs):
            result = self.world.run(*command, **kwargs)
            if command[:2] == ("docker", "kill"):
                raise KeyboardInterrupt("interrupted after kill")
            return result
        with patch.object(common, "run", side_effect=run):
            self.assert_failed("interrupted after kill", KeyboardInterrupt)
        self.assertTrue(self.world.follower["State"]["Running"])
        self.assertEqual(self.world.starts, 2)

    def test_failed_restart_does_not_loop_or_replace_data(self):
        def run(*command, **kwargs):
            if command[:2] == ("docker", "start"):
                raise RuntimeError("start refused")
            return self.world.run(*command, **kwargs)
        with patch.object(common, "run", side_effect=run) as run_mock:
            self.assert_failed("start refused")
            self.assertEqual(sum(c.args[:2] == ("docker", "start") for c in run_mock.call_args_list), 1)
        self.assertFalse(self.world.follower["State"]["Running"])

    def test_waits_for_new_ports_and_rpc_listener_after_restart(self):
        polls, rpc_failures = 0, 0
        def inspect(name):
            nonlocal polls
            info = self.world.inspect(name)
            if info["Id"] == "follower" and self.world.starts == 2:
                polls += 1
                if polls <= 3:
                    info["NetworkSettings"]["Ports"] = None
            return info
        def rpc(url, method, params, secret=None):
            nonlocal rpc_failures
            if self.world.starts == 2 and method == "engine_exchangeCapabilities" and rpc_failures == 0:
                rpc_failures += 1
                raise ConnectionResetError("listener starting")
            return self.world.rpc(url, method, params, secret)
        with patch.object(common, "inspect", side_effect=inspect), patch.object(common, "rpc", side_effect=rpc):
            self.execute()
        self.assertEqual(rpc_failures, 1)
        self.assertGreaterEqual(self.world.clock.now, 3)

    def test_missing_ports_time_out(self):
        def inspect(name):
            info = self.world.inspect(name)
            if info["Id"] == "follower" and self.world.starts == 2:
                info["NetworkSettings"]["Ports"] = {}
            return info
        with patch.object(common, "inspect", side_effect=inspect):
            self.assert_failed("Timed out waiting for follower HTTP/Engine")

    def test_unexpected_restart_while_waiting_for_rpc_is_rejected(self):
        polls = 0
        def inspect(name):
            nonlocal polls
            info = self.world.inspect(name)
            if info["Id"] == "follower" and self.world.starts == 2:
                polls += 1
                info["NetworkSettings"]["Ports"] = {}
                if polls > 2:
                    info["State"]["StartedAt"] = "unexpected-restart"
            return info
        with patch.object(common, "inspect", side_effect=inspect):
            self.assert_failed("exited, paused or restarted unexpectedly")

    def test_rpc_auth_error_is_not_retried(self):
        error = urllib.error.HTTPError("http://localhost", 401, "Unauthorized", {}, None)
        self.addCleanup(error.close)
        def rpc(url, method, params, secret=None):
            if self.world.starts == 2 and method == "engine_exchangeCapabilities":
                raise error
            return self.world.rpc(url, method, params, secret)
        with patch.object(common, "rpc", side_effect=rpc):
            self.assert_failed("Unauthorized", urllib.error.HTTPError)
        self.assertEqual(self.world.clock.now, 0)

    def test_resnap_or_absent_resume_log_is_not_a_pass(self):
        for output, message in [("Starting partial-state snap sync", "repeated snap bootstrap"),
                                ("", "No verified checkpoint-resume")]:
            with self.subTest(output=output):
                # Each invocation uses a new evidence directory even on the same running follower.
                with patch.object(common, "container_logs", return_value=output):
                    with self.assertRaisesRegex(RuntimeError, message):
                        self.execute()
        self.assertTrue(all(not (d / "result.json").exists() for d in self.directory.glob("abrupt-restart-*")))

    def test_entire_checkpoint_including_filter_identity_must_survive(self):
        def db(*args):
            cp = self.world.db(*args)
            if self.world.starts == 2 and args[1] == "PartialStateCheckpoints":
                cp["filter_hash"] = block(4)["hash"]
            return cp
        with patch.object(common, "db", side_effect=db):
            self.assert_failed("entire committed checkpoint")

    def test_missing_checkpoint_after_restart_is_rejected(self):
        def db(*args):
            if self.world.starts == 2 and args[1] == "PartialStateCheckpoints":
                return None
            return self.world.db(*args)
        with patch.object(common, "db", side_effect=db):
            self.assert_failed("Missing or incomplete")

    def test_journal_loss_after_restart_is_rejected(self):
        def run(*command, **kwargs):
            output = self.world.run(*command, **kwargs)
            if self.world.starts == 2 and command[-1] == "PartialStateTransitionJournals":
                return output.replace("0x1234", "0xbad")
            return output
        with patch.object(common, "run", side_effect=run):
            self.assert_failed("State or journal table checksums changed")

    def test_corrupt_recovered_rpc_is_rejected(self):
        def rpc(url, method, params, secret=None):
            if (self.world.starts == 2 and method == "eth_getStorageAt"
                    and url == common.endpoint(self.world.follower, 8545)):
                return block(2)["hash"]
            return self.world.rpc(url, method, params, secret)
        with patch.object(common, "rpc", side_effect=rpc):
            self.assert_failed("RPC comparison failed")

    def test_full_state_writes_after_advancement_are_rejected(self):
        def run(*command, **kwargs):
            output = self.world.run(*command, **kwargs)
            if self.world.cp["block_number"] == 11 and command[-1] in common.FULL_TABLES:
                return output.replace("0x1234", "0xbad")
            return output
        with patch.object(common, "run", side_effect=run):
            self.assert_failed("Full-state table checksums")

    def test_nonzero_finish_after_restart_is_rejected(self):
        def db(*args):
            if self.world.starts == 2 and args[1] == "StageCheckpoints":
                return {"block_number": 1}
            return self.world.db(*args)
        with patch.object(common, "db", side_effect=db):
            self.assert_failed("head advanced beyond genesis")

    def test_valid_engine_response_after_restart_is_rejected(self):
        def rpc(*args):
            if self.world.starts == 2:
                self.world.bad_engine = True
            return self.world.rpc(*args)
        with patch.object(common, "rpc", side_effect=rpc):
            self.assert_failed("Unexpected follower Engine")

    def test_producer_stall_after_restart_cannot_pass(self):
        def rpc(url, method, params, secret=None):
            if self.world.starts == 2 and method == "eth_getBlockByNumber" and params[0] == "latest":
                return block(self.world.head)
            return self.world.rpc(url, method, params, secret)
        with patch.object(common, "rpc", side_effect=rpc):
            self.assert_failed("Producer stopped advancing")

    def test_no_advancement_after_restart_times_out(self):
        def rpc(url, method, params, secret=None):
            old_cp = self.world.cp
            result = self.world.rpc(url, method, params, secret)
            if self.world.starts == 2 and method == "engine_forkchoiceUpdatedV3":
                self.world.cp = old_cp
            return result
        with patch.object(common, "rpc", side_effect=rpc):
            self.assert_failed("Timed out waiting for continued")

    def test_donor_restart_invalidates_run(self):
        def run(*command, **kwargs):
            output = self.world.run(*command, **kwargs)
            if command[:2] == ("docker", "start"):
                self.world.donor["State"]["StartedAt"] = "unexpected-restart"
            return output
        with patch.object(common, "run", side_effect=run), self.assertRaisesRegex(RuntimeError, "unexpectedly restarted"):
            self.execute()
        self.assertFalse((self.evidence() / "result.json").exists())

    def test_reorg_during_restart_is_not_misreported_as_success(self):
        def rpc(url, method, params, secret=None):
            if self.world.starts == 2 and method == "eth_getBlockByNumber" and params[0] == "0xa":
                return dict(block(10), hash=common.ZERO)
            return self.world.rpc(url, method, params, secret)
        with patch.object(common, "rpc", side_effect=rpc):
            self.assert_failed("Checkpoint does not match")
        comparison = json.loads((self.evidence() / "recovered-comparison.json").read_text())
        self.assertFalse(comparison["match"])


if __name__ == "__main__":
    unittest.main()

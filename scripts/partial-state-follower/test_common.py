"""Host-only regression tests; no Docker calls or node changes."""

import io
import json
import subprocess
import unittest
from unittest.mock import patch


import common


class LiveCheckpointReadTests(unittest.TestCase):
    def read(self):
        return common.db("producer", "StageCheckpoints", '"Finish"', "/data", "/genesis.json")

    def result(self, stdout="", stderr="", code=0):
        return subprocess.CompletedProcess([], code, stdout, stderr)

    def inconsistent(self):
        return self.result(stderr="Error: File is in an inconsistent state.\n\nLocation:\n"
                           "    /app/crates/cli/commands/src/common.rs:218:17\n", code=1)

    def read_only_repair(self):
        return self.result(stderr="Error: cannot get a writer on a read-only environment.\n\nLocation:\n"
                           "    /app/crates/cli/commands/src/common.rs:218:17\n", code=1)

    def test_success_uses_read_only_get_without_bypassing_checks(self):
        with patch.object(common.subprocess, "run", return_value=self.result('{"block_number":41}')) as run:
            self.assertEqual(self.read(), {"block_number": 41})
        run.assert_called_once_with(
            ("docker", "exec", "producer", "reth", "--quiet", "db", "--datadir", "/data",
             "--chain", "/genesis.json", "get", "mdbx", "StageCheckpoints", '"Finish"'),
            text=True, capture_output=True, timeout=15,
        )

    def test_retries_transient_inconsistency_and_reads_fresh_checkpoint(self):
        responses = [self.inconsistent(), self.result('{"block_number":42}')]
        with patch.object(common.subprocess, "run", side_effect=responses) as run, \
                patch.object(common.time, "sleep") as sleep, \
                patch.object(common.sys, "stderr", new_callable=io.StringIO) as stderr:
            self.assertEqual(self.read(), {"block_number": 42})
            self.assertIn("attempt 1/5", stderr.getvalue())
        self.assertEqual(run.call_count, 2)
        sleep.assert_called_once_with(2)

    def test_persistent_inconsistency_stops_after_five_attempts(self):
        with patch.object(common.subprocess, "run", return_value=self.inconsistent()) as run, \
                patch.object(common.time, "sleep") as sleep, \
                patch.object(common.sys, "stderr", new_callable=io.StringIO):
            with self.assertRaisesRegex(RuntimeError, "no checkpoint was accepted"):
                self.read()
        self.assertEqual(run.call_count, 5)
        self.assertEqual(sleep.call_count, 4)

    def test_retries_read_only_repair_without_changing_command(self):
        responses = [self.read_only_repair(), self.result('{"block_number":43}')]
        with patch.object(common.subprocess, "run", side_effect=responses) as run, \
                patch.object(common.time, "sleep") as sleep, \
                patch.object(common.sys, "stderr", new_callable=io.StringIO) as stderr:
            self.assertEqual(self.read(), {"block_number": 43})
            self.assertIn("attempt 1/5", stderr.getvalue())
        self.assertEqual(run.call_count, 2)
        expected = (
            "docker", "exec", "producer", "reth", "--quiet", "db", "--datadir", "/data",
            "--chain", "/genesis.json", "get", "mdbx", "StageCheckpoints", '"Finish"',
        )
        for call in run.call_args_list:
            self.assertEqual(call.args, (expected,))
            self.assertEqual(call.kwargs, {"text": True, "capture_output": True, "timeout": 15})
        sleep.assert_called_once_with(2)

    def test_persistent_read_only_repair_stops_after_five_attempts(self):
        with patch.object(common.subprocess, "run", return_value=self.read_only_repair()) as run, \
                patch.object(common.time, "sleep") as sleep, \
                patch.object(common.sys, "stderr", new_callable=io.StringIO):
            with self.assertRaisesRegex(RuntimeError, "no checkpoint was accepted"):
                self.read()
        self.assertEqual(run.call_count, 5)
        self.assertEqual(sleep.call_count, 4)

    def test_mixed_consistency_errors_share_one_retry_budget(self):
        responses = [self.inconsistent(), self.read_only_repair(), self.inconsistent(),
                     self.read_only_repair(), self.inconsistent()]
        with patch.object(common.subprocess, "run", side_effect=responses) as run, \
                patch.object(common.time, "sleep") as sleep, \
                patch.object(common.sys, "stderr", new_callable=io.StringIO):
            with self.assertRaisesRegex(RuntimeError, "no checkpoint was accepted"):
                self.read()
        self.assertEqual(run.call_count, 5)
        self.assertEqual(sleep.call_count, 4)

    def test_similar_read_only_errors_are_not_retried(self):
        failure = self.result(stderr="Error: cannot get a writer on a read-only environment: permission denied", code=1)
        with patch.object(common.subprocess, "run", return_value=failure) as run, \
                patch.object(common.time, "sleep") as sleep:
            with self.assertRaisesRegex(common.CommandError, "permission denied"):
                self.read()
        run.assert_called_once()
        sleep.assert_not_called()

    def test_unrelated_command_errors_are_not_retried(self):
        failure = self.result(stderr="Error: permission denied", code=1)
        with patch.object(common.subprocess, "run", return_value=failure) as run:
            with self.assertRaisesRegex(common.CommandError, "permission denied"):
                self.read()
        run.assert_called_once()

    def test_malformed_json_is_not_retried(self):
        with patch.object(common.subprocess, "run", return_value=self.result("bad json")) as run:
            with self.assertRaises(json.JSONDecodeError):
                self.read()
        run.assert_called_once()

    def test_timeout_is_not_retried(self):
        with patch.object(common.subprocess, "run", side_effect=subprocess.TimeoutExpired("docker", 15)) as run:
            with self.assertRaises(subprocess.TimeoutExpired):
                self.read()
        run.assert_called_once()

    def test_absent_key_is_not_replaced_with_a_checkpoint(self):
        with patch.object(common.subprocess, "run", return_value=self.result()) as run:
            self.assertIsNone(self.read())
        run.assert_called_once()


if __name__ == "__main__":
    unittest.main()

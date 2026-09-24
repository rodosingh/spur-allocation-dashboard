import http.client
import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

import app
import node_holder_bridge as holder
import scheduler
from request_store import RequestStore


POOLS = {
    "schemaVersion": 1,
    "bestPool": {"account": "amd-brain-models", "qos": "amd-brain-models-qos"},
    "pools": [
        {
            "account": "amd-brain-models",
            "qos": "amd-brain-models-qos",
            "priority": 10000,
            "preemptMode": "off",
            "maxWallMinutes": 1440,
            "maxSubmitPerUser": None,
        },
        {
            "account": "amd-brain-models",
            "qos": "amd-burst-qos",
            "priority": 100,
            "preemptMode": "cancel",
            "maxWallMinutes": 1440,
            "maxSubmitPerUser": 4,
        },
    ],
}


def valid_payload(**overrides):
    payload = {
        "mode": "chain",
        "strategy": "start",
        "jobName": "demo",
        "account": "amd-brain-models",
        "qos": "amd-brain-models-qos",
        "timeLimit": "12:00:00",
        "gpus": 8,
        "nodes": 1,
        "exclusive": True,
        "chainDepth": 3,
    }
    payload.update(overrides)
    return payload


class BridgeValidationTests(unittest.TestCase):
    def test_chain_request_normalizes_without_reimplementing_pool_choice(self):
        request = holder.validate_request(valid_payload(), POOLS)
        self.assertEqual(request["tag"], "demo")
        self.assertEqual(request["timeSeconds"], 12 * 3600)
        self.assertEqual(request["chainDepth"], 3)
        self.assertIsNone(request["cpus"])

    def test_partial_gpu_cpu_auto_derivation_is_used(self):
        with patch.object(holder, "_derive_cpus", return_value=118) as derive:
            request = holder.validate_request(valid_payload(gpus=4, cpus=None), POOLS)
        self.assertEqual(request["cpus"], 118)
        derive.assert_called_once_with(4, None)

    def test_account_and_qos_must_be_a_live_association(self):
        with self.assertRaisesRegex(ValueError, "not associated"):
            holder.validate_request(valid_payload(qos="amd-hyperloom-geak-qos"), POOLS)

    def test_burst_requires_both_preemption_and_priority_acknowledgements(self):
        burst = valid_payload(qos="amd-burst-qos", chainDepth=3)
        with self.assertRaisesRegex(ValueError, "acknowledge preemption"):
            holder.validate_request(burst, POOLS)
        burst["acceptPreemption"] = True
        with self.assertRaisesRegex(ValueError, "safety override"):
            holder.validate_request(burst, POOLS)
        burst["anyQos"] = True
        request = holder.validate_request(burst, POOLS)
        self.assertTrue(request["acceptPreemption"])
        self.assertTrue(request["anyQos"])

    def test_submit_limit_rejects_unreachable_chain_depth(self):
        burst = valid_payload(
            qos="amd-burst-qos",
            chainDepth=4,
            acceptPreemption=True,
            anyQos=True,
        )
        with self.assertRaisesRegex(ValueError, "chain depth can be at most 3"):
            holder.validate_request(burst, POOLS)

    def test_normal_jobs_cannot_impersonate_chain_names(self):
        with self.assertRaisesRegex(ValueError, "maintained-chain prefix"):
            holder.validate_request(
                valid_payload(
                    mode="normal", jobName=f"{holder.CHAIN_PREFIX}-demo"
                ),
                POOLS,
            )

    def test_priority_floor_follows_node_holder_configuration(self):
        burst = valid_payload(
            qos="amd-burst-qos", chainDepth=3, acceptPreemption=True
        )
        with patch.object(holder, "MIN_PRIORITY", 50):
            request = holder.validate_request(burst, POOLS)
        self.assertFalse(request["anyQos"])
        with patch.object(holder, "MIN_PRIORITY", 10_000):
            with self.assertRaisesRegex(ValueError, "below 10000"):
                holder.validate_request(burst, POOLS)

    def test_node_and_multinode_are_mutually_exclusive(self):
        with self.assertRaisesRegex(ValueError, "cannot be combined"):
            holder.validate_request(valid_payload(nodes=2, node="node026"), POOLS)

    def test_time_is_checked_against_live_qos_limit(self):
        with self.assertRaisesRegex(ValueError, "at most 1440 minutes"):
            holder.validate_request(valid_payload(timeLimit="2-00:00:00"), POOLS)

    @patch.object(holder.scheduler, "run_command")
    def test_chain_command_is_allowlisted_argv_and_carries_repin_env(self, run):
        run.return_value = "holder 123 submitted as chain 'demo'"
        request = holder.validate_request(valid_payload(gpus=4, cpus=118), POOLS)
        result = holder.run_chain_request(request)
        command = run.call_args.args[0]
        environment = run.call_args.kwargs["env"]
        self.assertEqual(command[0], str(holder.NODE_HOLDER))
        self.assertIn("start", command)
        self.assertNotIn("shell", command)
        self.assertEqual(command[command.index("-c") + 1], "118")
        self.assertEqual(environment["NODEHOLD_REPIN"], "1")
        self.assertEqual(result["jobIds"], ["123"])

    @patch.object(holder, "get_status")
    @patch.object(holder.scheduler, "run_command", return_value="done")
    def test_only_safe_chain_actions_are_callable(self, run, status):
        status.return_value = {"chains": [{"name": "hold-demo"}]}
        holder.run_chain_action("hold-demo", "release")
        self.assertEqual(run.call_args.args[0][-1], "release")
        self.assertEqual(run.call_args.kwargs["env"]["NODEHOLD_NAME"], "hold-demo")
        self.assertEqual(
            run.call_args.kwargs["env"]["NODEHOLD_CHAIN_FULL_NAME"],
            "hold-demo",
        )
        with self.assertRaisesRegex(ValueError, "Unsupported"):
            holder.run_chain_action("hold-demo", "tick")

    @patch.object(holder, "get_status", return_value={"chains": []})
    @patch.object(holder.scheduler, "run_command", return_value="done")
    def test_chain_action_refuses_unknown_or_inactive_names(self, run, _status):
        with self.assertRaisesRegex(ValueError, "not an active maintained chain"):
            holder.run_chain_action("hold-demo", "release")
        run.assert_not_called()

    @patch.object(holder, "_run_json")
    @patch.object(holder.scheduler, "get_queue")
    def test_status_discovers_active_chains_across_prefixes(self, queue, run_json):
        queue.return_value = [
            {"name": "hold-a"},
            {"name": "team-b"},
            {"name": "ordinary-job"},
        ]
        with tempfile.TemporaryDirectory() as directory, patch.object(
            holder, "STATE_DIR", Path(directory)
        ):
            Path(directory, "hold-a.conf").touch()
            Path(directory, "team-b.conf").touch()
            run_json.side_effect = lambda _command, prefix=None: {
                "chains": [{"name": prefix}]
            }
            payload = holder.get_status()
        self.assertEqual(
            [chain["name"] for chain in payload["chains"]],
            ["hold-a", "team-b"],
        )
        self.assertEqual(
            [call.kwargs["prefix"] for call in run_json.call_args_list],
            ["hold-a", "team-b"],
        )


class SchedulerTests(unittest.TestCase):
    @patch.object(scheduler, "run_command")
    def test_queue_parser_keeps_user_priority_and_reason(self, run):
        run.return_value = (
            "123|job|aditysin|amd-brain-models|amd-burst-qos|9000|PENDING|"
            "0:00|1-00:00:00|1|(Resources)|2026-09-24T01:00:00|N/A|gpu:8|236"
        )
        jobs = scheduler.get_queue("all")
        self.assertEqual(jobs[0]["user"], "aditysin")
        self.assertEqual(jobs[0]["priority"], 9000)
        self.assertEqual(jobs[0]["nodeListOrReason"], "(Resources)")

    @patch.object(scheduler, "run_command")
    def test_history_uses_spur_supported_whitespace_format(self, run):
        run.return_value = (
            "167006 hold-burst2 aditysin amd-hyperloom-geak CANCELLED 02:53:06 "
            "2026-09-22T09:45:19 2026-09-22T12:38:25 -1:0"
        )
        jobs = scheduler.get_recent_jobs()
        command = run.call_args.args[0]
        self.assertNotIn("--parsable2", command)
        self.assertEqual(jobs[0]["id"], "167006")
        self.assertEqual(jobs[0]["state"], "CANCELLED")

    @patch.object(scheduler, "run_command", return_value="456;cluster")
    def test_normal_submission_is_finite_and_uses_no_shell(self, run):
        job_id = scheduler.submit_normal(
            {
                "jobName": "demo",
                "account": "amd-brain-models",
                "qos": "amd-brain-models-qos",
                "nodes": 1,
                "timeLimit": "01:00:00",
                "timeSeconds": 3600,
                "gpus": 4,
                "cpus": 118,
                "node": None,
                "exclusive": False,
            }
        )
        command = run.call_args.args[0]
        self.assertEqual(job_id, "456")
        self.assertEqual(command[command.index("--wrap") + 1], "exec sleep 3540")
        self.assertNotIn("bash", command)

    @patch.object(scheduler, "run_command")
    @patch.object(scheduler, "get_queue")
    def test_chain_job_cannot_be_cancelled_as_normal(self, queue, run):
        queue.return_value = [{"id": "123", "name": "hold-demo"}]
        with self.assertRaisesRegex(PermissionError, "maintained chain"):
            scheduler.cancel_owned_job("123", {"hold-demo"})
        run.assert_not_called()


class RequestStoreTests(unittest.TestCase):
    def test_atomic_ledger_round_trip(self):
        with tempfile.TemporaryDirectory() as directory:
            store = RequestStore(Path(directory))
            written = store.record_request({"mode": "normal", "jobId": "123"})
            records = store.list_requests()
            self.assertEqual(records[0]["id"], written["id"])
            self.assertEqual(records[0]["jobId"], "123")


class FrontendContractTests(unittest.TestCase):
    def test_every_chain_action_has_plain_hover_help(self):
        source = (Path(__file__).parent / "static" / "app.js").read_text()
        for action in ("topup", "shrink", "arm", "clear", "tend", "untend", "release"):
            self.assertIn(f"{action}:", source)
        for action in ("topup", "shrink", "arm", "clear", "release"):
            self.assertIn(f"CHAIN_ACTION_HELP.{action}", source)
        self.assertIn('CHAIN_ACTION_HELP[chain.tended ? "untend" : "tend"]', source)
        self.assertIn('button.dataset.tooltip = title', source)
        self.assertIn('button.setAttribute("aria-label"', source)


class HandlerTests(unittest.TestCase):
    def setUp(self):
        self.original_token = app.CSRF_TOKEN
        app.CSRF_TOKEN = "live-token"
        self.server = app.ThreadingHTTPServer(("127.0.0.1", 0), app.DashboardHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        app.CSRF_TOKEN = self.original_token

    def request(self, method, path, body=None, token=None, origin=None):
        connection = http.client.HTTPConnection(*self.server.server_address)
        headers = {}
        encoded = None
        if body is not None:
            encoded = json.dumps(body)
            headers["Content-Type"] = "application/json"
        if token is not None:
            headers["X-CSRF-Token"] = token
        if origin is not None:
            headers["Origin"] = origin
        connection.request(method, path, body=encoded, headers=headers)
        response = connection.getresponse()
        payload = json.loads(response.read())
        connection.close()
        return response.status, payload

    def test_capabilities_and_live_csrf_are_available(self):
        status, payload = self.request("GET", "/api/capabilities")
        self.assertEqual(status, 200)
        self.assertIn("chain", payload["requestModes"])
        self.assertNotIn("tick", payload["chainActions"])
        self.assertEqual(payload["chainPrefix"], holder.CHAIN_PREFIX)
        self.assertEqual(payload["minPriority"], holder.MIN_PRIORITY)
        self.assertEqual(payload["user"], scheduler.USERNAME)
        status, payload = self.request("GET", "/api/csrf-token")
        self.assertEqual(payload["token"], "live-token")

    def test_mutation_requires_token_before_calling_backend(self):
        with patch.object(app, "create_request") as create:
            status, payload = self.request(
                "POST", "/api/requests", valid_payload(), token="stale"
            )
        self.assertEqual(status, 403)
        self.assertEqual(payload["error"], "Invalid CSRF token")
        create.assert_not_called()

    def test_untrusted_origin_is_rejected(self):
        with patch.object(app, "create_request") as create:
            status, payload = self.request(
                "POST",
                "/api/requests",
                valid_payload(),
                token="live-token",
                origin="https://evil.example",
            )
        self.assertEqual(status, 403)
        self.assertEqual(payload["error"], "Untrusted origin")
        create.assert_not_called()

    def test_read_only_mode_rejects_mutations_before_token_handling(self):
        with patch.object(app, "READ_ONLY", True), patch.object(
            app, "create_request"
        ) as create:
            status, payload = self.request(
                "POST", "/api/requests", valid_payload(), token="live-token"
            )
        self.assertEqual(status, 403)
        self.assertEqual(payload["error"], "Dashboard is running in read-only mode")
        create.assert_not_called()

    def test_chain_request_endpoint_returns_created(self):
        result = {"request": {"mode": "chain"}, "result": {"chainName": "hold-demo"}}
        with patch.object(app, "create_request", return_value=result) as create:
            status, payload = self.request(
                "POST", "/api/requests", valid_payload(), token="live-token"
            )
        self.assertEqual(status, 201)
        self.assertEqual(payload["result"]["chainName"], "hold-demo")
        create.assert_called_once()

    def test_chain_action_maps_validation_failure_to_bad_request(self):
        with patch.object(
            app, "run_chain_action", side_effect=ValueError("Unsupported chain action")
        ):
            status, payload = self.request(
                "POST",
                "/api/chains/action",
                {"chainName": "hold-demo", "action": "tick"},
                token="live-token",
            )
        self.assertEqual(status, 400)
        self.assertIn("Unsupported", payload["error"])


if __name__ == "__main__":
    unittest.main(verbosity=2)

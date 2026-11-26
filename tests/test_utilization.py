import json
import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import app
import gpu_sampler
import notifier


def sample(ts, *utils):
    return {"ts": ts, "gpus": [{"gpu": index, "util": util} for index, util in enumerate(utils)]}


class SamplerParserTests(unittest.TestCase):
    def test_amd_smi_value_dicts_and_partition_lists(self):
        payload = [
            {
                "gpu": 0,
                "usage": {"gfx_activity": {"value": 87, "unit": "%"}},
                "power": {"socket_power": {"value": 650, "unit": "W"}},
                "mem_usage": {"used_vram": {"value": 144000}, "total_vram": {"value": 288000}},
            },
            {"gpu": 1, "usage": {"gfx_activity": [10, 30]}, "power": {}, "mem_usage": {}},
        ]
        gpus = gpu_sampler.parse_amd_smi(payload)
        self.assertEqual(gpus[0], {"gpu": 0, "util": 87.0, "vramUsedPct": 50.0, "powerW": 650.0})
        self.assertEqual(gpus[1]["util"], 20.0)

    def test_amd_smi_wrapped_payload(self):
        gpus = gpu_sampler.parse_amd_smi({"gpu_data": [{"gpu": 3, "usage": {"gfx_activity": "5 %"}}]})
        self.assertEqual((gpus[0]["gpu"], gpus[0]["util"]), (3, 5.0))

    def test_rocm_smi(self):
        payload = {
            "card0": {"GPU use (%)": "42", "GPU Memory Allocated (VRAM%)": "7", "Current Socket Graphics Package Power (W)": "300.0"},
            "system": {"Driver version": "x"},
        }
        self.assertEqual(
            gpu_sampler.parse_rocm_smi(payload),
            [{"gpu": 0, "util": 42.0, "vramUsedPct": 7.0, "powerW": 300.0}],
        )


class SummaryTests(unittest.TestCase):
    def test_weights_low_stretch_and_gaps(self):
        start = 1_000_000
        samples = [sample(start + i * 300, 0, 0) for i in range(30)]
        samples += [sample(start + 9000 + i * 300, 90, 90) for i in range(4)]
        stats = app.summarize_samples(samples, 10, start + 9000 + 4 * 300)
        self.assertEqual(stats["samples"], 34)
        self.assertEqual(stats["longestLowHours"], 2.5)
        self.assertEqual(stats["currentLowHours"], 0)
        self.assertEqual(stats["gpuHours"], round(34 * 300 * 2 / 3600, 2))
        self.assertEqual(stats["idleGpuHours"], round(30 * 300 * 2 / 3600, 2))

    def test_gap_capped_and_errors_skipped(self):
        stats = app.summarize_samples(
            [sample(0, 0), {"ts": 300, "error": "amd-smi: missing"}, sample(10_000, 0)], 10, 10_300
        )
        self.assertEqual(stats["samples"], 2)
        self.assertEqual(stats["gpuHours"], round((300 + 300) / 3600, 2))
        self.assertEqual(stats["lastError"], "amd-smi: missing")


class BuildUtilizationTests(unittest.TestCase):
    def test_flags_low_jobs_and_lists_unreported(self):
        with tempfile.TemporaryDirectory() as root:
            now = 2_000_000_000
            user_dir = Path(root) / "alice"
            user_dir.mkdir()
            lines = [json.dumps(sample(now - 9000 + i * 300, 1, 2)) for i in range(30)]
            path = user_dir / "11.node-a.jsonl"
            path.write_text("\n".join(lines) + "\n")
            os.utime(path, (now, now))
            queue = [
                {"id": "11", "user": "alice", "state": "RUNNING", "name": "train", "qos": "q", "nodeListOrReason": "node-a", "elapsed": "2:30:00"},
                {"id": "12", "user": "alice", "state": "RUNNING", "name": "eval", "qos": "q", "nodeListOrReason": "node-b", "elapsed": "1:00:00"},
                {"id": "13", "user": "mallory", "state": "RUNNING", "name": "x", "qos": "q", "nodeListOrReason": "node-c", "elapsed": "1:00"},
            ]
            team = {"users": ["alice"], "focusQos": [], "utilization": dict(app.UTILIZATION_DEFAULTS), "error": None}
            with patch.object(app, "GPU_UTIL_DIR", Path(root)), patch.object(app, "load_team", return_value=team), patch.object(
                app.scheduler, "get_queue", return_value=queue
            ), patch.object(app.time, "time", return_value=now):
                data = app.build_utilization()
        jobs = {job["id"]: job for job in data["jobs"]}
        self.assertEqual(set(jobs), {"11", "12"})
        self.assertTrue(jobs["11"]["flagged"])
        self.assertEqual(jobs["11"]["avgUtil"], 1.5)
        self.assertFalse(jobs["12"]["reporting"])
        self.assertEqual(data["members"][0]["flaggedJobs"], 1)
        self.assertEqual(data["members"][0]["unreportedJobs"], 1)


class DigestTests(unittest.TestCase):
    def test_due_once_per_day_after_time(self):
        config = dict(notifier.DEFAULTS)
        state = {}
        self.assertFalse(notifier.digest_due(config, state, datetime(2026, 11, 26, 10, 59)))
        self.assertTrue(notifier.digest_due(config, state, datetime(2026, 11, 26, 11, 0)))
        state["lastDigestDate"] = "2026-11-26"
        self.assertFalse(notifier.digest_due(config, state, datetime(2026, 11, 26, 15, 0)))
        self.assertTrue(notifier.digest_due(config, state, datetime(2026, 11, 27, 11, 5)))

    def test_digest_mentions_owner(self):
        config = dict(notifier.DEFAULTS, emailDomain="amd.com")
        data = {"thresholds": {"lowUtilPercent": 10, "minLowHours": 2, "windowHours": 24}}
        jobs = [{"id": "11", "node": "n1", "state": "RUNNING", "avgUtil": 1.5, "longestLowHours": 2.5, "idleGpuHours": 20}]
        payload = notifier.build_digest("alice", jobs, data, config)
        self.assertEqual(payload["mention"]["email"], "alice@amd.com")
        self.assertIn("job <b>11</b>", payload["summary"])


if __name__ == "__main__":
    unittest.main()

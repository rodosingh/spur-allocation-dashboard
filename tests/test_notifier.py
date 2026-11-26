import unittest
from unittest.mock import patch

import notifier


def job(job_id, state="RUNNING", elapsed="0:05", user="alice", qos="amd-brain-models-qos"):
    return {
        "id": job_id,
        "name": "train",
        "user": user,
        "qos": qos,
        "state": state,
        "elapsed": elapsed,
        "nodes": 1,
        "nodeListOrReason": "node-1",
        "timeLeft": "23:55:00",
    }


class ElapsedTests(unittest.TestCase):
    def test_formats(self):
        self.assertEqual(notifier.elapsed_seconds("45"), 45)
        self.assertEqual(notifier.elapsed_seconds("5:07"), 307)
        self.assertEqual(notifier.elapsed_seconds("2:00:01"), 7201)
        self.assertEqual(notifier.elapsed_seconds("1-02:03:04"), 93784)
        self.assertEqual(notifier.elapsed_seconds("N/A"), 0)


class PollTests(unittest.TestCase):
    def setUp(self):
        self.config = dict(notifier.DEFAULTS, emails={"alice": "alice@amd.com"})
        self.sent = []
        patcher = patch.object(
            notifier, "send", side_effect=lambda card, config, summary: self.sent.append(summary) or True
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def poll(self, jobs, state):
        with patch.object(notifier, "fetch_team", return_value={"jobs": jobs}):
            return notifier.poll_once(self.config, state)

    def test_first_run_seeds_without_alerting(self):
        state = self.poll([job("1", elapsed="5:00:00")], None)
        self.assertEqual(self.sent, [])
        state = self.poll([job("1", elapsed="5:01:00")], state)
        self.assertEqual(self.sent, [])

    def test_start_then_single_reminder_then_prune(self):
        state = self.poll([job("2", state="PENDING")], {"jobs": {}})
        self.assertEqual(self.sent, [])
        state = self.poll([job("2")], state)
        self.assertEqual(len(self.sent), 1)
        self.assertTrue(self.sent[0].startswith("start 2"))
        state = self.poll([job("2", elapsed="1:59:00")], state)
        self.assertEqual(len(self.sent), 1)
        state = self.poll([job("2", elapsed="2:00:30")], state)
        state = self.poll([job("2", elapsed="2:05:00")], state)
        self.assertEqual(len(self.sent), 2)
        self.assertTrue(self.sent[1].startswith("reminder 2"))
        state = self.poll([], state)
        self.assertEqual(state["jobs"], {})

    def test_other_qos_ignored(self):
        self.poll([job("3", qos="amd-burst-qos")], {"jobs": {}})
        self.assertEqual(self.sent, [])

    def test_card_mentions_owner_with_email(self):
        card = notifier.build_card(job("4"), self.config, reminder=False)
        content = card["attachments"][0]["content"]
        self.assertEqual(content["msteams"]["entities"][0]["mentioned"]["id"], "alice@amd.com")
        self.assertIn("<at>alice</at>", content["body"][0]["text"])
        card = notifier.build_card(job("5", user="bob"), self.config, reminder=False)
        self.assertNotIn("msteams", card["attachments"][0]["content"])

    def test_email_domain_fallback(self):
        config = dict(self.config, emailDomain="amd.com")
        self.assertEqual(notifier.owner_email("alice", config), "alice@amd.com")
        self.assertEqual(notifier.owner_email("bob", config), "bob@amd.com")
        self.assertEqual(notifier.owner_email("bob", self.config), "")

    def test_token_flow_fields_without_card_mentions(self):
        config = dict(self.config, cardMentions=False)
        hostile = dict(job("6"), name="<script>x</script>")
        card = notifier.build_card(hostile, config, reminder=False)
        content = card["attachments"][0]["content"]
        self.assertNotIn("msteams", content)
        self.assertNotIn("<at>", content["body"][0]["text"])
        self.assertEqual(card["mention"], {"email": "alice@amd.com", "name": "alice"})
        self.assertIn("scancel 6", card["summary"])
        self.assertNotIn("<script>", card["summary"])


if __name__ == "__main__":
    unittest.main()

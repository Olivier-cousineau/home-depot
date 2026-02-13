import json
import os
import tempfile
import unittest

from src.orchestrator.tasks import build_homedepot_tasks, run_homedepot_tasks
from src.policy import load_homedepot_policy


class OrchestratorPolicyTests(unittest.TestCase):
    def setUp(self):
        self.stores = [{"store_number": "7001"}, {"store_number": "7002"}]
        self.skus = ["1001", "1002", "1003"]

    def test_default_mode_blocked_by_policy(self):
        tasks = build_homedepot_tasks(self.stores, self.skus, batch_size=2)
        policy = load_homedepot_policy("policy/sources.yml")
        metrics = run_homedepot_tasks(
            tasks,
            source_policy=policy,
            source_mode="scrape_mode",
            output_path="results/test_blocked.json",
        )
        self.assertGreater(metrics["tasks_total"], 0)
        self.assertEqual(metrics["tasks_skipped"], metrics["tasks_total"])

    def test_partner_api_mode_writes_normalized_snapshots(self):
        tasks = build_homedepot_tasks([{"store_number": "7001"}], ["1001"], batch_size=1)
        policy = load_homedepot_policy("policy/sources.yml")

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(
                [
                    {
                        "store_code": "7001",
                        "sku": "1001",
                        "price": 12.34,
                        "currency": "CAD",
                        "promo": 10.00,
                        "availability": "in_stock",
                        "source_ref": "partner:fixture",
                    }
                ],
                f,
            )
            feed_path = f.name

        try:
            os.environ["HOMEDEPOT_PARTNER_FEED"] = feed_path
            os.environ.pop("HOMEDEPOT_PARTNER_API_KEY", None)
            output_path = "results/test_partner.json"
            metrics = run_homedepot_tasks(
                tasks,
                source_policy=policy,
                source_mode="partner_api",
                output_path=output_path,
                max_rps=10,
                burst=10,
            )
            self.assertEqual(metrics["offers_written"], 1)
            with open(output_path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
            self.assertEqual(payload["offers"][0]["currency"], "CAD")
            self.assertEqual(payload["offers"][0]["retailer"], "homedepot_ca")
        finally:
            os.environ.pop("HOMEDEPOT_PARTNER_FEED", None)
            os.unlink(feed_path)


if __name__ == "__main__":
    unittest.main()
